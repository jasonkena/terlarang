# Terlarang
Various scripts not endorsed by Boston College IT services, for operating [Andromeda](https://www.bc.edu/bc-web/offices/its/services/research-services/linux-cluster.html).

## Usage
To reserve a node on a partition (taking into account maintenance reservations)
```bash
sqi.py <partition_name>
```

To kill a SLURM job when a process ends, backing up `tmux` session state, with Discord notifications
```bash
twatch.py
```
