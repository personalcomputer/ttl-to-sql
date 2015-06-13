SQL Database Turtle (.ttl) Importer
-----------------------------------

Small python script built to import Turtle RDF files into an indexed sql database to allow queries. It builds a naive database containing a single table of all triples.

Usage: `ttltosql.py TTL_FILE SQLITE3_FILE`

Dependencies: No extra work needed, everything required is included in a standard python distribution. (ensure it was compiled with sqlite3 support)

### Warning

This only parses the simplest of .ttl files. No prefixes supported at all. Check your data, it may already be largely in the simple supported N-Triples format. The converter drops unparsable lines as it goes, and logs their frequency in the progress update messages.

### Performance

Fairly terrible. This was meant to be about 1 hour worth of work (and it was), and because of that I did not really optimize except for 1 thing. It process about 200000 triples a minute, with the largest issues being the parser, and transactions overhead. If this tool recieves continued interest on my part, it will be rewritten in C++ and massively upgraded:

### Future Upgrades

- Better DB schema seperating items/predicates and including more RDF information.
- DB optimization (this is the 2nd largest perf issue, but I didn't even implemented the most simple of optimizations like prepared statements, no hdd flush, and in-memory journaling)
- Support for more sophisticated Turtle grammar.
- Better and faster grammar parser (largest perf issue)(raw UTF8, no translating from UTF8->python unicode->UTF8 (yes this takes a lot of time - I have no idea what python uses internally in-memory), multithread parsing, less branch mispredictions, zero-allocation zero-copy strings) (how much effort here depending on how the profiling results change after porting, I'm surprised at how poorly my python logic has performed)
- mmap on the .ttl