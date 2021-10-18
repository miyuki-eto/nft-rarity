import pandas as pd
from nft_metadata import fetch_collection_metadata
from nft_events import  fetch_collection_events

# Set pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('display.width', 2500)
pd.set_option("max_colwidth", 100)


# collection = "0x23FC142A6bA57a37855D9D52702fDA2EC4B4Fd53"  # etherphrocks
# collection = "0xB1bb22c3101E7653d0d969F42F831BD9aCCc38a5"  # KitPics
collection = "0xeE8C0131aa6B66A2CE3cad6D2A039c1473a79a6d"  # ethermerals

metadata = fetch_collection_metadata(collection)
print(metadata)

events = fetch_collection_events(collection)
print(events.head(10))
