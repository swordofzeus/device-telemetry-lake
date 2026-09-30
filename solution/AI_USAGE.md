# AI Assistance Disclosure


I use AI to write:
 1. Small granular functions after defining the function name and signature
 2. Writing tests for code I have written
 3. Brainstorming different ideas and tools
 4. Write the README from my git history and commit messages and notes as I built the solution
 5. Debug my SQL queries and sub sql queries

 I did NOT use AI to:
 1. Write entire class files or large parts of the solution at once

I also rejected advice given by the agent when I thought it didnt make sense - for example it pushed back on integrating a schema registry as overkill. Kafka at its core is just a big log file of binary data so if bad data goes in - you can waste entire days trying to clean it (which I have at companies that did not use one) and it almost always justifies the minor footprint of a new lightweight service.

