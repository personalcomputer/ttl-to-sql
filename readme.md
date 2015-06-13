SQL Database Turtle (.ttl and .nt) Importer
-------------------------------------------

Small python script built to import Turtle RDF (http://www.w3.org/TeamSubmission/turtle/) files into an indexed database. By default it builds a naive database containing a single 'triples' table, with an index on every column for full RDF graph querying.

```
Usage: ttltosql.py TTL_FILE SQLITE3_FILE [OPTION]...
Import N-Triple (Turtle subset) file into SQL database.

  --table      Database table name (default: triples)
```

Dependencies: No extra work needed, everything required is included in a standard python distribution. (ensure it was compiled with sqlite3 support)

### Warning

This only parses the simplest of .ttl files - the subset conforming to the N-Triples standard. No prefixes supported at all. Check your data, it may already be largely in the simple supported N-Triples format. The converter drops unparsable lines as it goes, and logs their frequency in the progress update messages. (explicitly convert .ttl to N-Triples .nt with `rapper -i turtle -o ntriples example.ttl > example.nt`)

### Conversion Performance

The converter was not initially created with performance in mind. It processes about 2.1 million triples a minute on a low end workstation (not counting indexing), with the largest issues being the parser, and transactions overhead. If this tool receives continued interest on my part, it will be rewritten in C++ and massively upgraded:

### Future Upgrades

- Better and faster grammar parser. (largest perf issue)(raw UTF8, no translating from UTF8->python unicode->UTF8 (yes this takes a lot of time - I have no idea what python uses internally in-memory), multithread parsing, more straightforward parser with less branching, zero-allocation zero-copy strings) (how much effort here depending on how the profiling results change after porting, I'm surprised at how poorly my python logic has performed)
- DB optimization
- POSSIBLY better DB schema including more RDF information (some details, like datatype URIs, are dropped.)
- Support for more sophisticated Turtle grammar
- mmap on the .ttl