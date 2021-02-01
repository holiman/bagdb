# BagDB

BagDB is a super simple datastore. It can't _quite_ be called a database, because it avoids implementing some of the most complex parts of an actual database. It's intended to be used in very particular circumstances. 

## Cheats

A 'proper' database is very complex, and has to solve several difficult problems: 

- Maintain an index. This serves several purposes: 
  - It decouples the `key`, which is the 'external' identifier, from the actual internal 
  storage representation. This gives the database freedom to store the data 'wherever' it fits best. 
  Having this decoupling means that when elements are deleted, other pieces of data can be moved to overwrite
  the freed element, and the database can be made more compact. 
  - The index allows for the external caller to 'forget' the existence of a piece of data, and later on, when 
  it discovers that it needs the data represented by `key`, it can query the database for it, and the database
  can look it up from disk and hand it back to the process. 
  
### What if? 

But what if we don't need to maintain an index? There are two obvious cavats here: 

- Without an index, we cannot move the data around after it's written. Compaction will not be possible. This can lead to 
fragmentation; where inserts/deletes fragments the buffer space, caused by differently-sized data-chunks, eventually deteriorating 
performance due to many small gaps spread out across the entire storage space. 
- Without an index, the external caller can no longer 'forget' about the data, and later
  query for a specific `key`. This means that the usability is limited to datasets which 
  can be / will be backed by an in-memory reference map. 
  
### What are the upsides? 

- Without an index, we don't need to maintain a complex index-implementation, but can be very low on resource consumption. No 
 extra allocated memory for index maintenance, no background threads for compaction work. 

### In practice

For the [proposal by @karalabe](https://gist.github.com/karalabe/821a1cd0270984a4198e904d34623b6c) about implementing a disk-backed transaction pool for [geth](https://github.com/ethereum/go-ethereum), 
we have very special circumstances: 

- The disk-backed storage is indeed backed by an in-memory structure of metadata. 
- The payloads will roughly equally heavy on `write`, `delete` and `read` operations.
- The data is somewhat transient, meaning that it's expected that the mean-storage time for a piece of data
 is measured in minutes rather than weeks. 
 
 ## Implementation
 
 The `bagdb` uses has the following API:
 
 - `Put(data []byte) uint64`. This operation stores the given `data`, and returns a 'direct' reference to where the data is stored. By 'direct', it means that there is no 
 indirection involved, the returned `key` is a direct reference to the `bucket` and `slot` where the data can later be found. 
   - The `bagdb` uses a set of `bucket`s. Each bucket has a dynamic number of `slots`, where each slot within a `bucket` is a fixed size. This design is meant to alleviate the 
   fragmentation problem: if a piece of `data` is `168 bytes`, and our bucket sizes are `100`, `200`, `400`, `800`, `1600` .... , then `bagdb` will choose the `bucket` 
   with `200` byte size. The `168` bytes of data will be placed into `bucket` `1`, at the first free slot. 
 - `Delete(key uint64)`. The `delete` operation will simply look up the `bucket`, and tell the `bucket` that the identified `slot` now is free for re-use. It will be overwritten
  during a later `Put`  operation. 
 - `Get(key uint64)`. Again, this is a very trivial operation: find the `bucket`, load the identified `slot`, and return. 
 