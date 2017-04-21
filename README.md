## Warning -  Untested Code Ahead
# Wrapper around database/sql
inspired by https://github.com/jmoiron/sqlx
## version
Alpha - WIP

## License
open


```GO

    ps,err:= picosql.New("driver","connection string")
    
    ps.Get(&target,"query",args)
    ps.Select(&target,"query",args)
    ps.Count("query",args)

    ps.Insert("query",args)
    ps.Update("query",args)

    ps.NamedInsertAll("query",args)
    ps.NamedUpdateAll("query",args)

    ps.Map("query",args)
    ps.Maps("query",args)
    ps.Slice("query",args)
    ps.Slices("query",args)

    ps.NamedExec("query",args)
    ps.Exec("query",args)
    ps.Query("query",args)
    ps.QueryRow("query",args)

    ps.Close()
    ps.Clone()
    ps.Ping()
    
## TODO
- Test
- DRY
- Tune
- Repeat