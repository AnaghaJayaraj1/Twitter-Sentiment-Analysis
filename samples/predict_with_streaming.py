
# coding: utf-8

# # Twitter Sentiment Analysis with Pyspark

# First step in any Apache Spark programming is to create a SparkContext. SparkContext is needed when we want to execute operations in a cluster. SparkContext tells Spark how and where to access a cluster. It is first step to connect with Apache Cluster. 

# In[6]:


from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession
import warnings

SCC_CHECKPOINT_PATH = "/Users/anujchaudhari/Desktop/256/project/samples/twitter_streaming/checkpoint"
STREAMING_SOCKET_IP = "192.168.0.100"
STREAMING_SOCKET_PORT = 5555
STREAMING_TIME_INTERVAL = 2

print("Creating SparkContext, SQLContext, StreamingContext...")

try:
    # create SparkContext on all CPUs available: in my case I have 4 CPUs on my laptop
    
    spark = SparkSession.builder.appName("twitter").getOrCreate()
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    print("Just created a SparkContext")
    
except ValueError:
    warnings.warn("SparkContext already exists in this scope")
    
    

# Create Spark Streaming Context

ssc = StreamingContext(sc, STREAMING_TIME_INTERVAL )
ssc.checkpoint(SCC_CHECKPOINT_PATH)
socket_stream = ssc.socketTextStream(STREAMING_SOCKET_IP, STREAMING_SOCKET_PORT)
lines = socket_stream.window(STREAMING_TIME_INTERVAL)

print("SparkContext Master: " + sc.master)


# In[8]:

print("Read and Load Data from 'clean_tweets.csv'")
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('clean_tweet.csv')


# In[9]:


type(df)


# In[10]:


df.show(5)


# In[11]:


df = df.dropna()


# In[12]:


df.count()


# After successfully loading the data as Spark Dataframe, we can take a peek at the data by calling .show(), which is equivalent to Pandas .head(). After dropping NA, we have a bit less than 1.6 million Tweets. I will split this in three parts; training, validation, test. Since I have around 1.6 million entries, 1% each for validation and test set will be enough to test the models.

# In[13]:

print("Spliting data into Training, Value and Test Set...")
(train_set, val_set, test_set) = df.randomSplit([0.01, 0.01, 0.98], seed = 2000)


# In[14]:


test_set.head(5)


# ## N-gram Implementation

# I had to use VectorAssembler in the pipeline, to combine the features I get from each n-grams.

# In[15]:


from pyspark.ml.feature import NGram, VectorAssembler
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def build_ngrams_wocs(inputCol=["text","target"], n=3):
    tokenizer = [Tokenizer(inputCol="text", outputCol="words")]
    ngrams = [
        NGram(n=i, inputCol="words", outputCol="{0}_grams".format(i))
        for i in range(1, n + 1)
    ]

    cv = [
        CountVectorizer(vocabSize=5460,inputCol="{0}_grams".format(i),
            outputCol="{0}_tf".format(i))
        for i in range(1, n + 1)
    ]
    idf = [IDF(inputCol="{0}_tf".format(i), outputCol="{0}_tfidf".format(i), minDocFreq=5) for i in range(1, n + 1)]

    assembler = [VectorAssembler(
        inputCols=["{0}_tfidf".format(i) for i in range(1, n + 1)],
        outputCol="features"
    )]
    label_stringIdx = [StringIndexer(inputCol = "target", outputCol = "label")]
    lr = [LogisticRegression(maxIter=100)]
    return Pipeline(stages=tokenizer + ngrams + cv + idf+ assembler + label_stringIdx+lr)


# ### Model Training

# In[16]:

print("Training the model(this may take some time)...")
trigramwocs_pipelineFit = build_ngrams_wocs().fit(train_set)
predictions_wocs = trigramwocs_pipelineFit.transform(val_set)
accuracy_wocs = predictions_wocs.filter(predictions_wocs.label == predictions_wocs.prediction).count() / float(val_set.count())

# print accuracy
print("Accuracy Score: {0:.4f}".format(accuracy_wocs))

# ### Test Set Prediction 

# In[17]:


# test_predictions = trigramwocs_pipelineFit.transform(test_set)
# test_accuracy = test_predictions.filter(test_predictions.label == test_predictions.prediction).count() / float(test_set.count())
# #test_roc_auc = evaluator.evaluate(test_predictions)

# # print accuracy, roc_auc
# print("Accuracy Score: {0:.4f}".format(test_accuracy))
# #print("ROC-AUC: {0:.4f}".format(test_roc_auc))


# ### Spark Streaming Tweet Handling

# In[18]:

print("Starting Spark Streaming...")

import time
from pyspark.sql import Row
from pyspark.sql import SparkSession


def tweet_cleaner_updated(text):
    return text

def processTweets(rdd):

    try:        
        spark = SparkSession.builder.appName("twitter").getOrCreate()
        
        tweet = rdd.collect()
        if len(tweet) != 0:
            tweet = list(tweet[0])
        else:
            tweet = []

        rows = []
        for i in range(len(tweet)):
            rows.append(Row(_c0=i,text=tweet[i],target=0))

        if len(rows) == 0:
            rows.append(Row(_c0=1,text="EMPTY TWEET",target=0))
            
        df = spark.createDataFrame(rows)
        df.registerTempTable("tweets")
        
    except Exception as e: 
        print(e)
    
lines = lines.map(lambda x: x.lower())
lines = lines.map(lambda x: x.replace(" rt " , " "))
lines = lines.map(lambda x: x.replace("\n" , " "))
lines = lines.reduce(lambda x,y : x + y)
lines = lines.map(lambda x: x.split(" $$$$$$ "))
lines = lines.map(lambda x: tweet_cleaner_updated(x))

lines.foreachRDD(lambda rdd: processTweets(rdd))



# In[19]:


ssc.start()


# ### Redis Queue Config

# In[ ]:


import redis

config = {
    'host' : 'localhost',
    'port' : 6379,
    'db' : 0
}

redis_object = redis.StrictRedis(**config)

channel = "tweet_prediction"


# ### Get Tweet Data from temp table and predict the sentiment

# In[ ]:


import time
import json

count = 0
predicted_tweets = 0
time.sleep( 2 )

while count < 500:
    
    print("********************* BLOCK " + str(count) + " ***********************\n")    
    
    df_all_tweets = sqlContext.sql( 'Select * from tweets' )
    
    predicted_tweets = trigramwocs_pipelineFit.transform(df_all_tweets).collect()
    
    for tweet in predicted_tweets:
        print("\n########################")
        print(tweet.text)
        print(tweet.prediction)

        # Send Data to Redis Queue
        message = {}
        message["text"] = json.dumps(tweet)
        message["sentiment"] = tweet.prediction
        message_body = json.dumps(message)
        message = '{message_body}'.format(**locals()).encode('UTF-8')
        
        redis_object.publish(channel, message)

    
    count = count + 1


# In[20]:


ssc.awaitTermination()

