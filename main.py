# import libraries
import pandas as pd
from neo_helper import NeoHelper

# import data
data = pd.read_csv('./movie_data.csv')

# create movies df excluding nulls
movies_df = data.dropna(axis=0, how='any').copy()

# combine actors
movies_df['actors'] = movies_df['actor_1_name'] + "|" + movies_df['actor_2_name'] + "|" + movies_df['actor_3_name']

helper = NeoHelper()
helper.connect_graph('neo4j', 'password')

helper.batch_load_data(movies_df)

# add nodes
helper.add_movie_node('Dustins Movie',90, 900,'English',10, 20, 2.1, 32)
helper.add_director_node('Carrie Chambers')
helper.add_actor_node('Dustin Chambers')
helper.add_actor_movie_relationship('Dustin Chambers', 'Dustins Movie')
helper.add_director_movie_relationship('Carrie Chambers', 'Dustins Movie')

# remove nodes
helper.remove_actor_movie_relationship('Dustin Chambers', 'Dustins Movie')
helper.remove_director_movie_relationship('Carrie Chambers', 'Dustins Movie')
helper.remove_movie_node('Dustins Movie')
helper.remove_director_node('Carrie Chambers')
helper.remove_actor_node('Dustin Chambers')

# queries
helper.query_who_acted_in('King Kong')
helper.query_who_directed('King Kong')
helper.query_actor_acted_in('Naomi Watts')
helper.query_acted_with('Naomi Watts')
helper.query_directed('Peter Jackson')