## Edited and adapted from David Cedar(2017)
import json
import hashlib
from time import gmtime, strftime

########  Product Class   ########
class Product:
    title = "e"
    price = "e"
    rating  = ""
    brand = "e"
    url = "e"
    image_url = "e"
    source_id = "asin"
    source_domain = "amazon"
    
    ## Inti
    def __init__(self, product=None ):
        #Initialise Object with a Json array instead of using Setters.
        if product != None:           
            self.title = product.title
            self.price = product.price
            self.rating = product.rating
            self.brand = product.brand
            self.url = product.url
            self.images = product.images
            self.source_id = product.source_id
            self.source_domain = product.source_domain
        print("New Product object Initialised in memory")
        
    ## Setters and Getters    
    def SetTitle(self, title):
        self.title = title.strip()

    def SetPrice(self, price):
        self.price = price
    
    def SetRating(self, rating):
        self.rating = rating
    
    def SetBrand(self, brand):
        self.brand = brand    
    
    def SetUrl(self, url):
        self.url = url
        
    def SetImage(self, url):
        if len(url) > 1:
            self.image_url = url
    
    def SetSourceID(self, id):
        #Strip removes white spaces and any other none standard chars
        self.source_id = id.strip()
    
    def SetSourceDomain(self, domain):
        self.source_domain = domain
    
    
    ## Support 
    def FormCompleted(self):
        print("len(title): ", len(self.title))
        print("len(price): ", len(self.price))
        print("rating: ", self.rating)
        if (len(self.title) > 1 or len(self.price) > 1 or len(self.rating) > 0):
            return True
        else:
            return False

    def ReturnJson(self):
        #Reutnrs Object infomation in form of a Json array
        m = hashlib.md5()
        m.update(self.source_id.encode('utf-8'))
        product = {
            'uid':        m.hexdigest(), #Set as main index in DynamoDB
            'title':      self.title,
            'price':      self.price,
            'rating':     self.rating,
            #'brand':      self.brand,
            'url':        self.url,
            #'image_url':     self.image_url,
            'sid':        self.source_id,
            #'domain':     self.source_domain,
            #'date':       strftime("%Y-%m-%d %H:%M:%S", gmtime())
        }
        return (product)

    def Print(self):
        print("### Printing Product ###")
        print(self.ReturnJson())
        print("###        end       ###")
