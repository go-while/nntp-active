# nntp-active

nntp-active keeps the active list of a usenet server in memory: one entry per newsgroup with its high and low article number and its posting status. The map is safe for concurrent use and is persisted as a text file.

It is used by [nntp-server](https://github.com/go-while/nntp-server).

## File format

`<ActiveDir>/local-mode.active`, one group per line:

```
<group> <hi> <lo> <status>
alt.test 4745 1 y
```

- **Fields:** `hi` is the highest article number and `lo` the lowest. A `lo` of 0 is loaded as 1, because some newsreaders refuse a group with low 0.
- **Invalid names:** lines with an invalid group name are skipped when the file is loaded.
- **Writing:** `Write_activemap` writes the whole map to a tmp file, fsyncs it and renames it over the active file. The active file is never missing, also not during a write.
- **Backups:** the previous versions are kept as `local-mode.active.old`, `.old.1` and `.old.2`.

## Usage

```go
import active "github.com/go-while/nntp-active"

var activemap active.ActiveMap

activemap.BootActive()
activemap.LoadActiveFile(cfg) // cfg *config.CFG from nntp-config, reads cfg.Settings.ActiveDir

// after an article got number msgnum in group:
activemap.SetActiveHi(group, msgnum) // hi = max(hi, msgnum)

data := activemap.GetActiveData(group) // ActiveData{Group, Num, Hi, Lo, Status, Update, Hash}
lines := activemap.GetActiveMap(0, false, active.BIGNUM64, false) // "group hi lo status", sorted

activemap.Write_activemap(&cfg.Settings, false)
```

Use `SetActiveHi` instead of `UpActiveValue(group, "hi")`. The old function counts `hi` up by one per call, so the value falls behind the real article numbers after a crash. `SetActiveHi` takes the article number from the overview and can not fall behind.

## License

[MIT](https://choosealicense.com/licenses/mit/)

## Author

[go-while](https://github.com/go-while)
