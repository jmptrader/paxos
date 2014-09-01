paxos
=====

a simple paxos library implemented by golang

MIT 6.824: Distributed Systems lab3 (http://css.csail.mit.edu/6.824/)

The application interface
--------------------------
px = paxos.Make(peers []string, me string)  
px.Start(seq int, v interface{}) -- start agreement on new instance  
px.Status(seq int) (decided bool, v interface{}) -- get info about an instance  
px.Done(seq int) -- ok to forget all instances <= seq  
px.Max() int -- highest instance seq known, or -1  
px.Min() int -- instances before this seq have been forgotten  

Algorithm pseudo-code
---------------------
proposer(v):
```
  while not decided:
    choose n, unique and higher than any n seen so far
    send prepare(n) to all servers including self
    if prepare_ok(n_a, v_a) from majority:
      v' = v_a with highest n_a; choose own v otherwise
      send accept(n, v') to all
      if accept_ok(n) from majority:
        send decided(v') to all
```

acceptor's state:  
  n_p (highest prepare seen)  
  n_a, v_a (highest accept seen)  

acceptor's prepare(n) handler:
```
  if n > n_p
    n_p = n
    reply prepare_ok(n_a, v_a)
  else
    reply prepare_reject
```

acceptor's accept(n, v) handler:
```
  if n >= n_p
    n_p = n
    n_a = n
    v_a = v
    reply accept_ok(n)
  else
    reply accept_reject
```

