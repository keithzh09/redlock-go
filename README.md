# redlock-go

Redis distributed locks in Golang


## Installation
```bash
go get github.com/keithzh09/redlock-go
```


## Usage
Lock:
```golang
import redlock github.com/keithzh09/redlock-go

ctx := context.Background()
err = rl.Lock(ctx, "key_name", time.Second*5)
```

UnLock:
```golang
import redlock github.com/keithzh09/redlock-go

ctx := context.Background()
rl.UnLock(ctx)
```

The lock only holds one resource(key) at a time like mutex, you can find sample code in redlock_test.go file.