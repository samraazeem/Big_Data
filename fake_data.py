from faker import Faker
from datetime import datetime
import time
import json

fake = Faker()

# def get_registered_user():
#     return {
#         "first_name": fake.first_name(),
#         "last_name": fake.last_name(),
#         "born_in": fake.year()
#     }

if __name__ == "__main__":
    with open("sample_data.json", "w") as outfile:

        for i in range(10):

            dict = {}
            timestamp = str(i)+"_"+str(datetime.now().strftime("%Y-%m-%dT:%H:%M:%S"))
            dict[timestamp] = fake.sentence()
            print(dict)
            outfile.write(json.dumps(dict))
            outfile.write('\n')
            time.sleep(5)