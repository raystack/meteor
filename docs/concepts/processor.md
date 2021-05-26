# Processor

A recipe can have multiple processors registered. A processor is basically a function that:
- expects a list of data
- processes the list
- returns a list

The result from a processor will be passed on to the next processor until there is no more processor.
