# Plan: Matching ais data to vessel id (vid)

Objective: Match the id (mid) in ais data to vessel id (vid)

# Input datasets

## ais data

* directory: data-dump/ais/stk
  * data-dump/ais/stk/trail: Contains ais data, partitioned by year
      * The datasets contains an id "mid" and actual ais data
      * The mid may refer to a vessel or a land-based ais-reciever, buoy and sea-pens
      * The records contain all vessels that are in the vicinity of Icelandic waters
      * For Icelandic vessels the records contain positions that are beyound Icelandic waters.
  * file mobile.parquet: Contains a table with:
      * "mid": 
      * "loid":
      " "glid":
      
The "mid" needs to mapped to a vessel id (vid). There are though the following 
known problems (there may be more):
  
  * There is not neccessarily a one-to-one match of mid-vid. Background:
    * Over time "mid" have in some cases "migrated" from one vessel to another
      * What often happens is that a vessel may be decomissioned. A new vessel may get the call-sign of the older vessel and then the "mid" is reused for the new vessel.
    * Over time same vessel may have been assigned to another "mid". I.e. same vessel can have two different mids, although not at the same time.

In the end I forsee that one can join ais data to a fishing trip. This probablly requires something like this

```
ais |>
  left_join(trip,
            by = join_by(vid, between(time, T1, T2)))
```
