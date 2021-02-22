# hangouts_parser
Google Hangouts parser for very large (or small, doesn't matter) json files

Recently I had an issue with parsing my google hangouts takeout file. At 1.6 GB, it was too large for my laptop to run with the standard json package and too large for any website I usually use to parse the takeout files. This was my solution :)

Basic information: this uses the ijson package which iteratively goes through the file.

To start: populate groups and contacts hash with get_chats(jsonfile), or load from a csv with import_groups(file) and import_contacts(file)

The following are the two main methods:

**get_chats(jsonfile)**
- Paramenter: JSON file, RETURNS: Nothing
- Creates a hash of all groups: key is chat ID, values is a hash with name and participants gaia_id
- Creates a hash of all contacts: key is gaia ids, value is username
- Uncomment print_chats to print the chats, uncomment print(contacts) to print the contacts

**get_messages(jsonfile, id, output=None)**
- Parameter: JSON file, chat ID - can be found in groups hash, Output file
- Returns: Dataframe of messages
