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

helper.add_movie_node('Dustins Movie',90, 900,'English',10, 20, 2.1, 32)
helper.remove_movie_node('Dustins Movie')

helper.add_director_node('Dustin Chambers')
helper.remove_director_node('Dustin Chambers')

helper.add_actor_node('Dustin Chambers')
helper.remove_actor_node('Dustin Chambers')

helper.batch_load_data(movies_df)

