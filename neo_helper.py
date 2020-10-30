class NeoHelper:
    from py2neo import Graph, Node

    def __init__(self):
        self.name = "NeoHelper"

    def connect_graph(self, username, password):
        graph = Graph("bolt://localhost:7687", user=username, password=password)
        self.graph = graph

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