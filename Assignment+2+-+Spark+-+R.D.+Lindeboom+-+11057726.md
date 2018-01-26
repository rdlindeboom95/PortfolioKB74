
In this weeks assignments you will join and aggregate data from a collection of user and restaurant in Mexico. The collections we use is from: https://archive.ics.uci.edu/ml/datasets/Restaurant+%26+consumer+data

We have placed three files in the folder of this tutorial: 
- `userprofile.csv`: describes several atributes of the users
- `geoplaces2.csv`:  describes several attributes of the restaurants
- `rating_final.csv`: describes how the users rated the restaurant, the food and the service on a scale [0-2]

## part 1 ##

The first assignment is to estimate whether people with a larger budget are generally more satisfied with the restaurant they choose.

Create an RDD `rating` of the restaurant rating (the first rating) in `rating_final.csv`.


```python
filename = 'rating_final.csv'
rating = sc.textFile(filename)
ratingheader = rating.first()
```

Also create an RDD `userbudget` in which you load the `userID` and `budget` from `userprofiles.csv`.


```python
filename = 'userprofile.csv'
BudgetUser = sc.textFile(filename)
BudgetUserheader = BudgetUser.first()

def splitkey(x):
    s = x.split(',')
    return (s[0], s[17])

BudgetUser = BudgetUser.map(splitkey)
BudgetUser.take(5)
```




    [('userID', 'budget'),
     ('U1001', 'medium'),
     ('U1002', 'low'),
     ('U1003', 'low'),
     ('U1004', 'medium')]



To join the `userbudget` with the `userrating`, you must convert the `ratings` to a (userID, rating) structure (we don't need the placeID for this assignment).


```python
def splitkey(x):
    s = x.split(',')
    return (s[0], s[2])

rating = rating.map(splitkey)
```

Join `useratings` and `userbudget`, and map the result to a `(budget, rating)` structure. Don't forget to convert rating to an int (with `int()`).


```python
budget_rating = BudgetUser.join(rating)
budget_rating = budget_rating.values()
budget_rating = budget_rating.filter(lambda x: x[1] != 'rating')
budget_rating = budget_rating.map(lambda x: (x[0], int(x[1])))
budget_rating.take(5)
```




    [('low', 1), ('low', 1), ('low', 2), ('low', 1), ('low', 1)]



Group the result by budget (the key), and compute the average rating. To compute the average of a list `l` in python you can divide `sum(l)` by `len(l)`.


```python
A = (0,0)
grouped = budget_rating.aggregate.ByKey(A, lambda a,b: (a[0] + b, a[1] + ),
                                        lambda a,b : (a[0] + b[1]))
finalResult = grouped.mapValues(lambda v: v[0]/v[1]).collect()
print(finalResult)
```


      File "<ipython-input-15-63eb7fad7287>", line 2
        grouped = budget_rating.aggregate.ByKey(A, lambda a,b: (a[0] + b, a[1] + ),
                                                                                 ^
    SyntaxError: invalid syntax



Indeed, it seems that users with a higher budget are more satisfied with the restaurants they visit.

## Part 2 ##

The next assignment is to estimate whether users ratings are affected by the distance between where they live and where the restaurant is. 

We want to compute the distance between the user's home and the restaurant for every rating. Both positions can be looked up from the userprofiles and the places.

Create an RDD `userpos` with the userID, latitude and longitude from `userprofiles.csv`. To use the location, put latitude and longitude inside a tuple.


```python
filename = 'userprofile.csv'
ProfilesUser = sc.textFile(filename)

def splitkey(x):
    s = x.split(',')
    return (s[0], (s[1], s[2]))

userpos = ProfilesUser.map(splitkey)

userpos = userpos.filter(lambda x: x[0] != 'usedID')

userpos.take(5)
```




    [('userID', ('latitude', 'longitude')),
     ('U1001', ('22.139997', '-100.978803')),
     ('U1002', ('22.150087', '-100.983325')),
     ('U1003', ('22.119847', '-100.946527')),
     ('U1004', ('18.867', '-99.183'))]



Create a broadcast variable that contains a dictionary from which you can lookup a users position based on their ID.


```python
bc_userpos = sc.broadcast(userpos.collectAsMap())

```

Also create an RDD `placepos` that contains the ID and position of places in `geoplaces2.csv`.


```python
filename = 'geoplaces2.csv'
geoplaces = sc.textFile(filename)

def splitkey(x):
    s = x.split(',')
    return (s[0], (s[1], s[2]))

placepos = geoplaces.map(splitkey)
placepos = placepos.filter(lambda x: x[0] != 'placeID')
placepos.take(5)
```




    [('134999', ('18.915421', '-99.184871')),
     ('132825', ('22.1473922', '-100.983092')),
     ('135106', ('22.1497088', '-100.9760928')),
     ('132667', ('23.7526973', '-99.1633594')),
     ('132613', ('23.7529035', '-99.165076'))]



And create a similar broadcast variable to lookup the position of a place based in it's ID.


```python
bc_geopos = sc.broadcast(placepos.collectAsMap())
```

To compute the distance an accurate approximation is the Vincenty distance in the geopy library (use `pip install geopy` to install). 

Here is an example to compute the distance:


```python
from geopy.distance import vincenty
vincenty((31.8300167,35.0662833), (31.83,35.0708167)).meters
# lukt mij niet :*()
```




    429.16765838976664



Now, map the ratings, so that you retrieve the position of the user and the position of the restaurant, and compute the vincenti distance between them. Output the distance and rating.


```python
# zie opdracht hierboven
```




    [('2', 693.4067254748844),
     ('2', 806.8757681276663),
     ('2', 1036.3302977034452),
     ('1', 729.1540436901495),
     ('1', 80.87798147571374)]



Now average the distance per rating.


```python
# zie opdracht hierboven
```




    [('1', 10620.704846132321),
     ('0', 32532.24377565323),
     ('2', 27352.013001884887)]



It seems that there is no linear relation between the distance to the restaurant and the given rating.
