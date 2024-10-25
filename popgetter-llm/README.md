## General approach

- We want to build a natural language interface for popgetter 
- This should allow the user to excute queries like 
- "Produce me a file containing the population breakdown by age of men living in Manchester as a geojson"

To do this we need to 
- Create some tools that interface with the popgetter library to execute generated recipes 
- Create an llm agent to generate those recipes from the query 
- Allow that agent to be able to search our metadata for relevant metrics / geometries / whatevs for the query

## Flow

### User interaction
0: Populate the vector database with the embedings for each metric description.

user enters prompt -> 
(Prompt the llm to consider the users query and produce a list of datasets that will be required to answer it)->
we put the prompt through the embedding api to produce vector ->  
we find the top n vectors in the database which are close to this ->
we populate a new query with the metadata from those results along with the original query and prompt the generation of the recipe
We run that recipe through popgetter lib to generate the result