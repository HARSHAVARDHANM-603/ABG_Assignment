# import statement
import pickle

# correct names of flights
correct_flights = ['Indigo', 'Spicejet', 'Air india', 'Air vistara']

# writing the initial data into a pickle file
with open('flights.pkl', 'wb') as file:
    pickle.dump(correct_flights, file)

