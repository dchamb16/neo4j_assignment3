class NeoHelper:
    from py2neo import Graph, Node, NodeMatcher, Relationship

    def __init__(self):
        self.name = "NeoHelper"

    def connect_graph(self, username, password):
        graph = self.Graph("bolt://localhost:7687", user=username, password=password)
        self.graph = graph

    def add_movie_node(self, title, duration, gross, language, budget, usersCount, imdbScore, movieFbLikes):
        node = self.Node('Movie', 
            title=title, duration=duration, gross=gross, language=language, usersCount=usersCount,
            imdbScore=imdbScore, movieFbLikes=movieFbLikes)
        
        self.graph.create(node)

        print('Node successfully created')

    def remove_movie_node(self, title):
        self.graph.run('''
            MATCH (m:Movie)
            WHERE m.title = $movie_title
            DETACH DELETE m
            ''', movie_title = title)

        print('Movie successfully deleted')

    def create_movie_nodes(self, df):
        # create movies nodes
        for i in range(df.shape[0]):
            title = df.iloc[i, :]['movie_title']
            duration = int(df.iloc[i, :]['duration'])
            gross = int(df.iloc[i, :]['gross'])
            language = df.iloc[i, :]['language']
            budget = int(df.iloc[i, :]['budget'])
            usersCount = int(df.iloc[i, :]['num_user_for_reviews'])
            imdbScore = df.iloc[i, :]['imdb_score']
            movieFbLikes = int(df.iloc[i, :]['movie_facebook_likes'])
            
            node = self.Node("Movie",
                        title = title,
                        duration = duration,
                        gross = gross,
                        language = language,
                        budget = budget,
                        usersCount = usersCount,
                        imdbScore = imdbScore,
                        moveiFbLikes = movieFbLikes
                    )
        
            self.graph.create(node)

        print('Movie nodes successfully created!')

    def create_plot_nodes(self, df):
        all_plots = set()
        for i in range(df.shape[0]):
            plot_list = df.iloc[i, :]['plot_keywords'].split("|")
            for plot in plot_list:
                all_plots.add(plot)

        for plot in all_plots:
            node = self.Node("Plot", name=plot)
            self.graph.create(node)

        print("Plot nodes successfully created!")

    def add_director_node(self, director_name):
        node = self.Node('Director', name=director_name)
        self.graph.create(node)
        print('Director successfully added')

    def remove_director_node(self, director_name):
        self.graph.run('''
            MATCH (d:Director)
            WHERE d.name = $director_name
            DETACH DELETE d
            ''', director_name = director_name)

        print('Director successfully removed')

    def create_director_nodes(self, df):
        # create director nodes
        all_directors = set()
        for i in range(df.shape[0]):
            director = df.iloc[i, :]['director_name']
            all_directors.add(director)
                
        for director in all_directors:
            node = self.Node("Director", name = director)
            self.graph.create(node)

        print("Director nodes successfully created!")

    def create_genre_nodes(self, df):
        # create genre nodes
        all_genres = set()
        for i in range(df.shape[0]):
            genre_list = df.iloc[i, :]['genres'].split("|")
            for genre in genre_list:
                all_genres.add(genre)

        for genre in all_genres:
            node = self.Node("Genre", name=genre)
            self.graph.create(node)

        print("Genre nodes successfully created!")

    def add_actor_node(self, actor_name):
        node = self.Node('Actor', name=actor_name)
        self.graph.create(node)
        print('Actor successfully added')

    def remove_actor_node(self, actor_name):
        self.graph.run('''
            MATCH (a:Actor)
            WHERE a.name = $actor_name
            DETACH DELETE a
            ''', actor_name = actor_name)

        print('Actor successfully removed')

    def create_actor_nodes(self, df):
        # create actors nodes
        all_actors = set()
        for i in range(df.shape[0]):
            actor_list = df.iloc[i, :]['actors'].split("|")
            for actor in actor_list:
                all_actors.add(actor)

        for actor in all_actors:
            node = self.Node("Actor",name=actor)
            self.graph.create(node)

        print("Actor nodes successfully created!")

    def add_actor_movie_relationship(self, actor_name, movie_title):
        matcher = self.NodeMatcher(self.graph)
        movie_node = matcher.match('Movie', title=movie_title).first()
        actor_node = matcher.match('Actor', name=actor_name).first()
        relationship = self.Relationship(actor_node, 'ACTED_IN', movie_node)
        self.graph.create(relationship)
        print('Actor to Movie relationship successfully created')

    def remove_actor_movie_relationship(self, actor_name, movie_title):
        query = '''
        MATCH (a:Actor)-[r:ACTED_IN]->(m:Movie)
        WHERE a.name = $actor_name and m.title = $movie_title
        DELETE r
        '''
        self.graph.run(query, actor_name = actor_name, movie_title = movie_title)
        print('Actor to Movie relationship successfully removed')

    def create_actor_movie_relationships(self, df):
        # Actors
        for i in range(df.shape[0]):
            title = df.iloc[i, :]['movie_title']
            matcher = self.NodeMatcher(self.graph)
            movie_node = matcher.match("Movie", title=title).first()
            
            actors = df.iloc[i, :]['actors'].split("|")
            for actor in actors:
                actor_node = matcher.match("Actor", name=actor).first()
                relationship = self.Relationship(actor_node, "ACTED_IN", movie_node)
                self.graph.create(relationship)
                
        print("Relationships successfully created!")

    def create_movie_genre_relationship(self, df):
        # Genres
        for i in range(df.shape[0]):
            title = df.iloc[i, :]['movie_title']
            matcher = self.NodeMatcher(self.graph)
            movie_node = matcher.match("Movie", title=title).first()
                
            genres = df.iloc[i, :]['genres'].split("|")
            for genre in genres:
                genre_node = matcher.match("Genre", name=genre).first()
                relationship = self.Relationship(movie_node, "IN_GENRE", genre_node)
                self.graph.create(relationship)

        print("Relationships created successfully!")

    def create_movie_plot_relationship(self, df):
        # Plots
        for i in range(df.shape[0]):
            title = df.iloc[i, :]['movie_title']
            matcher = self.NodeMatcher(self.graph)
            movie_node = matcher.match("Movie", title=title).first()
            
            plots = df.iloc[i, :]['plot_keywords'].split("|")
            for plot in plots:
                plot_node = matcher.match("Plot", name=plot).first()
                relationship = self.Relationship(movie_node, "HAS_PLOT", plot_node)
                self.graph.create(relationship)        

        print("Relationships created successfully!")

    def add_director_movie_relationship(self, director_name, movie_title):
        matcher = self.NodeMatcher(self.graph)
        movie_node = matcher.match('Movie', title=movie_title).first()
        director_node = matcher.match('Director', name=director_name).first()
        relationship = self.Relationship(director_node, 'DIRECTED', movie_node)
        self.graph.create(relationship)
        print('Director to Movie relationship successfully created')

    def remove_director_movie_relationship(self, director_name, movie_title):
        query = '''
        MATCH (d:Director)-[r:DIRECTED]->(m:Movie)
        WHERE d.name = $director_name and m.title = $movie_title
        DELETE r
        '''
        self.graph.run(query, director_name = director_name, movie_title = movie_title)
        print('Director to Movie relationship successfully removed')

    def create_director_movie_relationship(self, df):
        # Directors
        for i in range(df.shape[0]):
            title = df.iloc[i, :]['movie_title']
            matcher = self.NodeMatcher(self.graph)
            movie_node = matcher.match("Movie", title=title).first()
            
            director = df.iloc[i, :]['director_name']
            director_node = matcher.match("Director", name=director).first()
            relationship = self.Relationship(director_node, "DIRECTED", movie_node)
            self.graph.create(relationship)       
                

        print("Relationships created successfully!")

    def batch_load_data(self, df):
        self.create_movie_nodes(df)
        self.create_plot_nodes(df)
        self.create_director_nodes(df)
        self.create_genre_nodes(df)
        self.create_actor_nodes(df)
        self.create_actor_movie_relationships(df)
        self.create_movie_genre_relationship(df)
        self.create_movie_plot_relationship(df)
        self.create_director_movie_relationship(df)
        print('Finished Loading Data')