# CODE BY KAITLYN YANG 2021
# Google Hangouts parser for large json files (works for files of 1.6 GB at least)
# Can provide a transcript of messages and DataFrame for analysis

import ijson
import pandas as pd
import time
import csv
import numpy as np

contacts = {}
groups = {}
result = None

# Formatting printing of chats - 
# mode = 0 prints all
# mode = 1 prints only solo chats
# mode = 2 prints only group chats
def print_chats(chats, mode=0):
    if mode < 0 or mode > 2:
        return -1
    
    # Tabbed formatting
    print ("{:<45}{:<20}{:<15}".format('Chat_ID','Name', 'Participants'))
    for id,data in chats.items():
        if mode < 2 and data['name'] == 'solo':
            participants = str.join(', ',(list(contacts[x] for x in data['participants'])))
            print ("{:<45}{:<20}{:<15}".format(id,data['name'],participants))

        elif (mode == 0 or mode == 2) and data['name'] != 'solo':
            participants = str.join(', ',(list(contacts[x] for x in data['participants'])))
            print ("{:<45}{:<20}{:<15}".format(id,data['name'],participants))
    
    return 0

# Creates contact list with each name connected to a gaia_id
# Creates a hash of all groups: key is chat ID, values is a hash with name and participants gaia_id
# Name is either chat name or 'solo' | participants is a list of gaia ids
def get_chats(jsonfile):
    with open(jsonfile, "r", encoding="utf-8") as f:

        # Opens all conversation items - defines the group
        chats = ijson.kvitems(f, "conversations.item.conversation")
        
        # For each group
        for k,v in chats:
            # Makes it easier to get data this way
            if k == 'conversation':
                try:

                    # Gets type of chat - "GROUP" has key "name", solo does not
                    name = ('solo' if v['type'] != 'GROUP' else v['name'])

                    # Gets all the participants in the group
                    participants = []
                    for p in v['participant_data']:
                        # Updates contacts dict
                        if p['id']['gaia_id'] not in contacts.keys() or contacts[p['id']['gaia_id']] == 'Unknown':
                            try:
                                contacts[p['id']['gaia_id']] = p['fallback_name']
                            except KeyError:
                                contacts[p['id']['gaia_id']] = 'Unknown_'+p['id']['gaia_id']

                        participants.append(p['id']['gaia_id'])
                    
                    groups[v['id']['id']] = {'name': name, 'participants':participants, 'messages': count}
                except KeyError:
                    # if there is a key error, ignores the group
                    pass
        
        #print (contacts)
        #print_chats (groups, mode=2) 

# Captures all the prefixes, events, and values for all the events of a given chat id
# Writes it to a file if an output is provided
def get_truncated_json(jsonfile, id, output=None):

    capture = False
    all = []

    with open(jsonfile, "r", encoding="utf-8") as f:
        messages = ijson.parse(f)

        for prefix, event, value in messages:
            if (prefix, value) == ("conversations.item.events.item.conversation_id.id", id):
                capture = True
            elif capture and (prefix, event) == ("conversations.item.events.item", "end_map"):
                capture = False
            
            if capture:
                all.append([prefix,event,value])
    
    sorted = pd.DataFrame(all, columns=['prefix', 'event', 'value'])
    #print (sorted)

    if output != None:
        np.savetxt(output,sorted, fmt="%s <%s> %s", encoding="utf-8")
    
# For a given json file and a chat ID (which can be found using get_chats), writes all messages to a 
# specified output file and returns a pandas DataFrame object with the messages sorted by date written

# OUTPUT FILE: formatted = date time <sender> message
# Senders can be
#   - participant fallback name (indicates a regular message)
#   - member_change (indicates someone was added, left, or removed)
#   - start_call (indicates the start of a call, message formatted "_____ started a call")
#   - end_call (indicates the end of a call, message formatted "in a call for _____ seconds")
#   - conversation_rename (indicates conversation renamed, message formatted "____ renamed the Hangout to _____")
# Images are just written as the link to the image provided in the json file

def get_messages(jsonfile, id, output=None):
    
    capture = False
    type = "Regular"
    
    all = []

    with open(jsonfile, "r", encoding="utf-8") as f:
        # By using ijson.parse, there is no memory usage BESIDES the variable all. This is different than
        # using ijson.items or ijson.kvitems as there is no way to filter only the events for a given
        # conversation ID so it will load all messages ever sent into memory instead.        
        messages = ijson.parse(f)
        
        message = ["sender", "timestamp", ""]
        
        for prefix, event, value in messages:
            if (prefix, value) == ("conversations.item.events.item.conversation_id.id", id):
                # The conversation ID is the first piece of data provided for an event, so if the conversation
                # id matches the target, can freely capture all data until the end_map for the event
                capture = True

            elif capture and (prefix) == ("conversations.item.events.item.sender_id.gaia_id"):
                # Captures the sender; if the gaia_id is not in contacts, sender is "Unknown_<gaia_id>"
                try:
                    message[0] = contacts[value]
                except KeyError:
                    message[0] = "Unknown_"+value

            elif capture and (prefix) == ("conversations.item.events.item.timestamp"):
                # Captures the timestamp, written in epoch microseconds
                message[1] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(value)/1000000))

            elif capture and (prefix) == ("conversations.item.events.item.chat_message.message_content.segment.item.text"):
                # A single chat message may be broken up into multiple segments which needs to be added together
                message[2] += value

            elif capture and (prefix) == ("conversations.item.events.item.chat_message.message_content.attachment.item.embed_item.plus_photo.url"):
                # Captures image link
                message[2] += value

            elif capture and prefix == "conversations.item.events.item.membership_change.type":
                # If there is a membership change, the type is "JOIN" if someone is added
                # Change the type of message for formatting at the end
                type = "Member"
                if value == "JOIN":
                    message[2] = " added "
                else:
                    message[2] = " removed "

            elif capture and prefix == "conversations.item.events.item.conversation_rename.new_name":
                # Conversation rename, changing the type of message being sent
                type = "Rename"
                message[2] = " renamed the Hangout to " + value

            elif capture and prefix == "conversations.item.events.item.membership_change.participant_id.item.gaia_id":
                # If a membership change is occuring, this captures who is getting added or kicked
                try:
                    message[2] += contacts[value] + "."
                except KeyError:
                    message[2] += "Unknown_" + value + "."

            elif capture and (prefix) == ("conversations.item.events.item.hangout_event.hangout_duration_secs"):
                # End call events include a duration value
                type = "End Call"
                message[2] = "in a call for " + value + " seconds"

            elif capture and (prefix, value) == ("conversations.item.events.item.hangout_event.event_type", "START_HANGOUT"):
                # Start call events include who started the call
                type = "Start Call"
                message[2] = " started a call"

            elif capture and (prefix, event) == ("conversations.item.events.item", "end_map"):
                # At the end map, check the type and format message accordingly
                if type == "Member":
                    all.append([message[1], 'member_change', message[0]+message[2]])
                elif type == "Rename":
                    all.append([message[1], 'conversation_rename', message[0]+message[2]])
                elif type == "Start Call":
                    all.append([message[1], 'start_call', message[0]+message[2]])
                elif type == "End Call":
                    all.append([message[1], 'end_call', message[2]])
                else:
                    all.append([message[1], message[0], message[2]])

                message[2] = ""
                type = "Regular"
                capture = False
                member = False

    result = pd.DataFrame(all, columns=['timestamp', 'sender', 'message']).sort_values('timestamp')
    
    if output != None:
        np.savetxt(output,result, fmt="%s <%s> %s", encoding="utf-8")

    return result

# Returns the total message counts for all chats
def get_chat_message_counts(jsonfile):
    count = {}
    result = {}
    with open(jsonfile, "r", encoding="utf-8") as f:
        messages = ijson.kvitems(f, "conversations.item.events.item")
        for k,v in messages:
            if k == 'conversation_id':
                if v['id'] in count.keys():
                    count[v['id']] += 1
                else:
                    count[v['id']] = 1

    for k,v in count.items():
        try:
            if groups[k]['name'] == 'solo':
                result[contacts[groups[k]['participants'][1]]] = v
            else:
                result[groups[k]['name']] = v
        except KeyError:
            result[k] = v

    return result

# Helpful for large files if you don't want to have it read the entire file again just to determine
# the contacts or groups. Also, useful as a reference later on.
def import_contacts(file):
    with open(file, "r", encoding="utf-8", newline='') as f:
        reader = csv.reader(f, delimiter=' ', quotechar="|")
        for row in reader:
            contacts[row[0]] = row[1]

def export_contacts(file):
    with open(file, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for k,v in contacts.items():
            writer.writerow([k,v])

def import_groups(file):
    with open(file, newline='', encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=' ', quotechar="|")
        for row in reader:
            row[2] = row[2][1:-1].replace("'","")
            groups[row[0]] = {'name':row[1], 'participants':row[2].split(", ")}
    

def export_groups(file):
    with open(file, 'w', newline='', encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for k,v in groups.items():
            writer.writerow([k, v['name'], v['participants']]) 

# BASIC DATA ANALYSIS CODE
# Given a DataFrame, returns the amount of messages sent by each sender in a dict
def get_sender_count(data):
    count = {}
    for x in data['sender']:
        try:
            count[x] += 1
        except KeyError:
            count[x] = 1
    
    return count

# Given a DataFrame, returns the amount of messages sent on each date in a dict
def get_date_count(data):
    count = {}
    
    for x in data['timestamp']:
        date = x.split(" ")[0]
        # month = x.split(" ")[0][:-3]
        try:
            count[date] += 1
        except KeyError:
            count[date] = 1

    return count

# Writes the dictionary to a csv; readable with excel
def dict_to_csv(data, file, header):
    with open(file, 'w', newline='', encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for k,v in data.items():
            writer.writerow([k,v])

# Given a DataFrame, dict of message count by date, participant name (NOT ID), output file
# returns the TOTAL number of messages sent by a user overtime
# To get total_count, use get_date_count(data) method
def userdata_overtime(data, total_count, name, output = None):
    
    user_count = get_count_date(data[data['sender'] == name])

    total = 0
    count = {}
    for k,_ in total_count.items():
        try:
            total += user_count[k]
        except:
            pass
        count[k] = total
    
    if output != None:
        dict_to_csv(count, output,[])

    return count

