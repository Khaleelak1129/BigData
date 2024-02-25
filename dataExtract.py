import requests
import redis
import json
import matplotlib.pyplot as plt



class DataLoader:
    def __init__(self,RedisAPi=""):
        self.RedisApi = RedisAPi

    def fetch_data_from_api(self, url, headers=None):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  
            data = response.json() 
            return data
        except Exception as e:
            print("Error fetching data:", e)
            return None
        
    def load_data_to_redis(self,data, redis_host=None, redis_port=None,redis_username = None,redis_password = None, redis_db=None):
        try:
            # Connect to Redis
            r = redis.Redis(host=redis_host, port=redis_port,
                            username=redis_username,password=redis_password,)
            
            # Convert the data to JSON string
            json_data = json.dumps(data)
            
            # Store the JSON data in Redis
            r.set('data_key', json_data)
            
            print("Data loaded into Redis successfully.")
        except Exception as e:
            print("Error loading data into Redis:", e)
            

    
    def read_data_from_redis(self, redis_host=None, redis_port=None, redis_username=None, redis_password=None, redis_db=None):
        try:
            # Connect to Redis
            r = redis.Redis(host=redis_host, port=redis_port, username=redis_username, password=redis_password)
            
            # Retrieve data from Redis
            json_data = r.get('data_key')
            
            # Decode JSON data
            if json_data:
                data = json.loads(json_data)
                return data
            else:
                print("No data found in Redis.")
                return None
        except Exception as e:
            print("Error reading data from Redis:", e)
            return None

class Analytics:
    def __init__(self, data):
        self.data = data

    def search(self, book_name=None):
        if book_name:
            # Search for books containing the provided name in their title
            results = [book for book in self.data if book_name.lower() in book["title"].lower()]
            return results
        else:
            return self.data

    def plotdata(self, variable):
        if variable == "price":
            # Extracting price data
            data_values = [book["price"] for book in self.data]
            xlabel = 'Price'
            title = 'Histogram of Prices'
        elif variable == "stars":
            # Extracting stars data
            data_values = [book["stars"] for book in self.data]
            xlabel = 'Stars'
            title = 'Histogram of Stars'
        else:
            print("Invalid variable for plotting.")
            return

        # Plotting histogram
        plt.hist(data_values, bins=10, edgecolor='black')
        plt.xlabel(xlabel)
        plt.ylabel('Frequency')
        plt.title(title)
        plt.grid(True)
        plt.show()

    def aggregate(self, variable):
        if variable == "availability":
            # Calculate average availability
            total_availability = sum(book[variable] for book in self.data)
            avg_availability = total_availability / len(self.data)
            return avg_availability
        elif variable == "price":
            # Calculate average price
            total_price = sum(book[variable] for book in self.data)
            avg_price = total_price / len(self.data)
            return avg_price
        # You can add more cases for different variables as needed
        else:
            return None


if __name__ == "__main__":
    dl = DataLoader()
    url = "https://apis-ugha.onrender.com/books"
    data = dl.fetch_data_from_api(url)
    print("extracted data successfully")

    # Load data into Redis
    redis_host = 'redis-15378.c326.us-east-1-3.ec2.cloud.redislabs.com'
    redis_port = 15378  # Your Redis Cloud port
    redis_password = 'yagna'  # Your Redis Cloud password
    redis_db = 'Yagna-free-db' 
    username = 'default'
    dl.load_data_to_redis(data,redis_host=redis_host,
                          redis_port=redis_port,
                          redis_username=username,
                          redis_password=redis_password,
                          redis_db=redis_db)

    # Read data from Redis
    redis_data = dl.read_data_from_redis(redis_host=redis_host,
                                         redis_port=redis_port,
                                         redis_username=username,
                                         redis_password=redis_password,
                                         redis_db=redis_db)

    if redis_data:
        #print("Data read from Redis:", redis_data)
        # Initialize Analytics object with the retrieved data
        analytics = Analytics(redis_data)

        # Example usage of search method
        search_results = analytics.search("Scott Pilgrim")
        print("Search results:", search_results)

        # Example usage of aggregate method
        avg_availability = analytics.aggregate("availability")
        print("Average availability:", avg_availability)

        # Example usage of plotdata method
        # Replace variable1 and variable2 with actual variables you want to plot
        analytics.plotdata("price")
    else:
        print("Failed to read data from Redis.")
