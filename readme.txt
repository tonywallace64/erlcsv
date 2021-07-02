Objective:

This project contains a number of small tools for processing csv documents.
The intention is that by keeping the tools small with a consistent interface so that
demand drive pipes can be constructed.

Module interface:
All modules which can be formed into pipes are modelled on gen_servers. A module
may or may not be acutally constructed as a gen_server, but it will be started
like one and receive calls like one.  Every module exports a read call which returns
either data or eof.

To keep with this convention the line_accumulator, takes a mode parameter which
can be either 'raw' or 'tuple', and this determines the format returned in response
to a read.

Advantages:
By separating file reading (module file_reader) from csv parsing the parser module may
take its data from a non-file input without modification, for example it could be fed
csv data from a remote source.

Demand driven interfaces also tend to be more efficient that normal pipe as data is
only read when it is acutually required.

