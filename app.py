from streamlit.connections import ExperimentalBaseConnection
from streamlit.runtime.caching import cache_data
import streamlit as st
from connect_mongodb import MongoDBConnection
import json 

def main():
        
    conn = st.experimental_connection(name = "mongodb", 
                                      database_url = "mongodb+srv://demo_read:demo_read@cluster0useast1.rhpmhuc.mongodb.net/?retryWrites=true&w=majority",
                                      type = MongoDBConnection, 
                                      db_name = "streamlit_hackathon", collection_name = "us_senators")
    
    st.markdown("# The United States Senator Directory + MongoDB Backend")
    
    st.markdown("""
                In this Streamlit app, we showcase the seamless integration of MongoDB with Streamlit using the `ExperimentalBaseConnection` module that works on top of the official pymongo package. 
                
                For demonstration purposes, this application uses MongoDB M0 free cluster with limited storage. The dataset used is a directory of the current US senate. 
                
                The app utilizes a custom `MongoDBConnection` class to establish a connection to a MongoDB server and interact with its databases and collections. 
                
                By leveraging the power of Streamlit's new `experimental_connection` capabilities, we provide an intuitive and interactive user interface to use data from MongoDB 
                and display the results in real-time. 
                
                The codebase for this application can be found from [this github repository](https://github.com/fraclad/streamlit-mongodb-connection). 
                """)
    
    st.markdown("## Querying from a MongoDB collection")
    user_input_query = st.text_input("MongoDB query (you can try different states or filter by different fields!)", '{"state": "TX"}')
    query = json.loads(user_input_query)
    if st.button("query data"):
        data = conn.query_many(query)
        st.write(data)
    
    st.markdown("## Inserting records into a MongoDB collection")
    sample_insert_query = """
    {
    "name": "Yeetbruh Saleh",
    "state": "TX",
    "age": 25,
    "email": "deeeeezn@lol.com"
    }
    """
    user_input_insert = st.text_input("MongoDB insert query (you must change the query a little bit for this to work)", sample_insert_query)
    query_insert = json.loads(user_input_insert)
    if st.button("insert record"):
        try:
            conn.insert_one(query_insert)
        except:
            conn.insert_many(query_insert)
        st.markdown("record(s) inserted! you can use the query feature above to see the record(s) you just inserted!")
            
    st.markdown("## Deleting records from a MongoDB collection")
    st.markdown("""
                For sanity and security reasons, this feature was not implemented in an interactive manner.
                However, the methods `.delete_one()` and `.delete_many()` were included in the source code in the github repository for those who are interested. 
                """)
        
if __name__ == "__main__":
    main()