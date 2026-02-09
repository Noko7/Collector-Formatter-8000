# VMware Cluster Summary Generator

A tool for Nutanix SEs to quickly extract sizing data from VMware/Nutanix collector exports.

---

## Quick Start

1. **Drop collector files** into the `input_files/` folder (Excel format - `.xlsx` or `.xls`)
2. **Run the script**: Double-click or run `python cluster_summary.py`
3. **Get your results** in the `results/` folder

That's it. Each collector file generates its own summary Excel file.

---

## What You Get

For each collector file, the tool generates:

| Output | Description |
|--------|-------------|
| `cluster_summary_*.xlsx` | Formatted Excel with per-cluster hardware details |
| `validation_*.txt` | Processing log with row counts and sanity checks |

### The Excel Output Contains:

- **Total Summary Table** - All clusters aggregated into one view (top of file)
- **Per-Cluster Sections** - Each cluster gets its own table with:
  - Cluster-level metrics (VMs, vCPU, RAM, storage, ratios)
  - Host hardware details (vendor, model, CPU, RAM, storage per host)

---

## When to Use This

✅ **Pre-sales sizing** - Quick hardware inventory before creating a Sizer scenario  
✅ **Multi-cluster environments** - Get a consolidated view across clusters  
✅ **Hardware refresh projects** - Document existing host specs for replacement planning  
✅ **Proposal prep** - Extract key metrics for customer presentations  

---

## When NOT to Use This

❌ **Performance sizing** - This is capacity data only, not performance/utilization metrics  
❌ **Storage deep-dive** - Use RVTools or the collector directly for detailed datastore analysis  
❌ **Single-VM analysis** - This aggregates at cluster/host level, not per-VM  

---

## Important Notes on the Data

### What's Included
- Host hardware specs (vendor, model, CPU model, cores, RAM, NICs)
- VM counts and vCPU totals per cluster
- VM memory allocation (not utilization)
- vDisk provisioned and consumed storage
- vCPU to pCore ratios

### What's NOT Included
- CPU/Memory utilization percentages
- IOPS or throughput metrics
- Network bandwidth data
- Historical trends

### Storage Numbers
- **Provisioned Storage** = Total vDisk size allocated to VMs
- **Consumed Storage** = Actual space used on datastore
- **Capacity vDisk** = vDisk capacity (may differ from provisioned for thin disks)
- All values in TiB (binary terabytes)

### RAM Numbers
- Host RAM shown in GB (as reported by VMware)
- VM RAM shown in GiB (converted from MiB)

---

## Folder Structure

```
automation/
├── cluster_summary.py      # The main script
├── input_files/            # DROP COLLECTOR FILES HERE
│   └── (your .xlsx files)
└── results/                # OUTPUT GOES HERE
    └── cluster_summary_*.xlsx
```

---

## Requirements

- Python 3.8+
- pandas (`pip install pandas`)
- openpyxl (`pip install openpyxl`)

First-time setup:
```
pip install pandas openpyxl
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "No input files found" | Make sure Excel files are in `input_files/` folder |
| Missing required sheets | Collector must have vCluster, vHosts, vInfo, vCPU, vMemory, vDisk, vPartition sheets |
| Permission denied | Close any open Excel files in the results folder |
| openpyxl warning | Run `pip install openpyxl` |

---

## Legacy Mode

If you have individual CSV files (vCluster.csv, vHosts.csv, etc.) instead of a collector Excel file, just place them in the same folder as the script. The tool will process them directly.

---

## Questions?

This tool uses MOID-based joins (not cluster/VM names) so it handles obfuscated collector data correctly. The output is meant as a starting point for Sizer - always validate key numbers with the customer.
