import boto3

### ------------------------------------
###    Save Products to DynamoDB Class
### ------------------------------------
class SaveProducts:
     
    #Constructor function
    def __init__ (self,products_buffer):
        self.products_buffer = products_buffer
        ### Save prodct into database
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.create_table(TableName='AmazonProducts_first',
        KeySchema=[
            {
                'AttributeName': 'uid',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'title',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'uid',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'title',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }

        )

    def put_product(self,title, uid, price,url, rating, dynamodb=None):
        if not dynamodb:
            dynamodb = boto3.resource('dynamodb')
    
        response = self.table.put_item(
            Item = 
            {
                'uid': uid,
                'title': title,
                'info': {
                    'price':price,
                    'url': url,
                    'rating': rating
                }
           }
        )
        return response
    def update(self):
        print("[*] Buffer Size: {}".format(len(self.products_buffer)))
        for i in range(len(self.products_buffer)):
            #print ("Does it even try to upload")
            #try:
            #print(self.products_buffer[0].ReturnJson())
            #self.table.put_item(Item = self.products_buffer[0].ReturnJson()) #Save oldest product
            
            prod_dict = self.products_buffer[i].ReturnJson()
            title =  prod_dict['title']
            ID =  prod_dict['uid']
            price = prod_dict['price']
            url_link =  prod_dict['url']
            rating =  prod_dict['rating']
            prod_resp = self.put_product(title,ID,price,url_link,rating)
            
            print("[**] Successfully Uploaded Product")
