"""
File:        vsphere.py
Author:      Kevin Auberson
Created:     2026-05-10
Description: Client for vCenter that retrieves the real measured power of
             ESXi hosts and estimates the power consumption of running
             VMs based on the CPU MHz ratio.
"""

import atexit
import os
import ssl

from cache import cache
from pyVim.connect import Disconnect, SmartConnect
from pyVmomi import vim


class VSphere:
    """Client for vCenter, exposing host power and per-VM estimations."""

    def __init__(self):
        self.host = os.environ["VCENTER_HOST"]
        self.user = os.environ["VCENTER_USER"]
        self.password = os.environ["VCENTER_PASSWORD"]
        self.ttl = 60

        # Connection objects (lazy-initialized on first use)
        self._si = None
        self._content = None
        self._perf_manager = None
        self._counter_ids = {}

    def _connect(self):
        """
        Connect to vCenter once and cache the session.

        Also builds a name -> ID mapping for the performance counters
        we will query later (CPU, memory, power).
        """
        if self._si is not None:
            return

        # vCenter uses a self-signed certificate.
        # Validation is intentionally disabled here; mitigated by the network
        # being isolated and access restricted by firewall.
        ctx = ssl._create_unverified_context()  # nosec B323
        self._si = SmartConnect(
            host=self.host,
            user=self.user,
            pwd=self.password,
            sslContext=ctx,
        )
        atexit.register(Disconnect, self._si)

        self._content = self._si.RetrieveContent()
        self._perf_manager = self._content.perfManager

        wanted = [
            "cpu.usage.average",
            "cpu.usagemhz.average",
            "mem.consumed.average",
            "power.power.average",
        ]
        for c in self._perf_manager.perfCounter:
            full_name = f"{c.groupInfo.key}.{c.nameInfo.key}.{c.rollupType}"
            if full_name in wanted:
                self._counter_ids[full_name] = c.key

    def _query_stats(self, entity, counter_names):
        """
        Query performance counters for a single entity (Host or VM).

        Args:
            entity: A vim.HostSystem or vim.VirtualMachine object.
            counter_names: List of counter names like "cpu.usage.average".

        Returns:
            A dict {counter_name: float_value}. Empty if no data.
        """
        ids = [self._counter_ids[n] for n in counter_names if n in self._counter_ids]
        metric_ids = [
            vim.PerformanceManager.MetricId(counterId=i, instance="") for i in ids
        ]

        spec = vim.PerformanceManager.QuerySpec(
            entity=entity,
            metricId=metric_ids,
            intervalId=20,
            maxSample=1,
        )
        results = self._perf_manager.QueryPerf(querySpec=[spec])

        if not results or not results[0].value:
            return {}

        id_to_name = {
            self._counter_ids[n]: n for n in counter_names if n in self._counter_ids
        }
        output = {}
        for val in results[0].value:
            name = id_to_name.get(val.id.counterId)
            if name and val.value:
                output[name] = float(val.value[0])
        return output

    def _get_all(self, vim_type):
        """
        Retrieve all objects of a given vSphere type.

        Args:
            vim_type: A vim type like vim.HostSystem or vim.VirtualMachine.

        Returns:
            A list of objects of the requested type.
        """
        view = self._content.viewManager.CreateContainerView(
            self._content.rootFolder, [vim_type], True
        )
        objects = list(view.view)
        view.Destroy()
        return objects

    def get_host_power(self):
        """
        Get the real measured power of every ESXi host.

        Returns:
            A dict { host_name: {"watts": float, "total_mhz": int} }.
            'watts' comes from power.power.average (hardware sensor).
            'total_mhz' is the host's total CPU capacity (cores * clock).
        """
        cached = cache.get("vsphere_hosts")
        if cached is not None:
            return cached

        self._connect()

        hosts = {}
        for host in self._get_all(vim.HostSystem):
            stats = self._query_stats(host, ["power.power.average"])
            hw = host.summary.hardware
            hosts[host.name] = {
                "watts": stats.get("power.power.average", 0.0),
                "total_mhz": hw.cpuMhz * hw.numCpuCores,
            }

        cache.set("vsphere_hosts", hosts, self.ttl)
        return hosts

    def get_vm_estimated_watts(self):
        """
        Estimate the power consumed by each running VM.

        The estimation uses the VM's CPU usage in MHz divided by the SUM of
        all powered-on VMs' CPU usage on the same host (i.e. the host's
        actual concurrent usage), applied to the host's measured Watts.

        This differs from a naive division by the host's theoretical peak
        capacity (cpuMhz * numCpuCores): that denominator stays constant
        regardless of how busy the host actually is, so on a lightly loaded
        host it silently under-attributes almost all of the host's real
        power draw. Using the sum of ACTUAL usage instead guarantees the
        host's measured Watts are fully and proportionally distributed
        among the VMs that are actually running at that moment — matching
        physical reality, where power doesn't vanish just because the host
        isn't at 100% of its theoretical maximum.

        It remains a linear approximation that ignores memory and I/O
        contributions, and does not attribute any power to a host's
        near-zero idle floor when no VM is using CPU.

        Returns:
            A list of dicts, one per powered-on VM, each with:
            - name (str): VM name
            - host (str): ESXi host running the VM
            - cpu_mhz (float): VM CPU usage in MHz
            - memory_mib (float): VM memory consumption in MiB
            - watts (float): estimated power in Watts
        """
        cached = cache.get("vsphere_vms")
        if cached is not None:
            return cached

        self._connect()
        hosts = self.get_host_power()

        # First pass: collect raw stats and group actual CPU usage by host,
        # since the denominator now depends on ALL VMs on that host.
        raw_vms = []
        usage_by_host: dict[str, float] = {}
        for vm in self._get_all(vim.VirtualMachine):
            if vm.runtime.powerState != "poweredOn":
                continue

            stats = self._query_stats(
                vm,
                [
                    "cpu.usagemhz.average",
                    "mem.consumed.average",
                ],
            )
            cpu_mhz = stats.get("cpu.usagemhz.average", 0)
            mem_mib = stats.get("mem.consumed.average", 0) / 1024
            host_name = vm.runtime.host.name

            raw_vms.append(
                {
                    "name": vm.name,
                    "host": host_name,
                    "cpu_mhz": cpu_mhz,
                    "memory_mib": mem_mib,
                }
            )
            usage_by_host[host_name] = usage_by_host.get(host_name, 0.0) + cpu_mhz

        # Second pass: attribute each host's measured Watts proportionally
        # to each VM's share of the host's ACTUAL total usage this cycle.
        vms = []
        for vm in raw_vms:
            host_data = hosts.get(vm["host"], {"watts": 0, "total_mhz": 1})
            host_actual_mhz = usage_by_host.get(vm["host"], 0.0)

            if host_actual_mhz > 0:
                watts = (vm["cpu_mhz"] / host_actual_mhz) * host_data["watts"]
            else:
                # Host measured >0 W but no VM shows CPU usage this cycle
                # (e.g. sampling gap): split the host's power evenly rather
                # than attributing 0 to everyone.
                n_vms_on_host = sum(1 for v in raw_vms if v["host"] == vm["host"])
                watts = host_data["watts"] / n_vms_on_host if n_vms_on_host else 0

            vms.append(
                {
                    "name": vm["name"],
                    "host": vm["host"],
                    "cpu_mhz": vm["cpu_mhz"],
                    "memory_mib": vm["memory_mib"],
                    "watts": watts,
                }
            )

        cache.set("vsphere_vms", vms, self.ttl)
        return vms


# Standalone test: python vsphere.py
if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    vs = VSphere()
    print(vs.get_vm_estimated_watts())
