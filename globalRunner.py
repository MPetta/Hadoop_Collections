#!/usr/bin/env python
# coding: utf-8

# In[29]:


import os
import sys
import string
import time
import threading
from collections import ChainMap
 
# Setup from input
start = time.perf_counter()
inputDirectory = '/Users/inputFolder'
outPutDirectory = '/Users/outputFolder'
lettersPerPage = 1000

fileNameList = []
fullWordKeyList = []
listOfAllDicts = []

def textIndex(inputDirectory,outPutDirectory,lettersPerPage):
    with os.scandir(inputDirectory) as files:
        for file in files:
            # Open the file in read mode 
            text = open(file, "r")
            # Create neccesary objects
            d = dict()
            pageNumber = 1
            numberOfCharacters = 0
            filename = os.path.basename(file)
            # Loop through each line of the file 
            for line in text:
                # Remove the leading spaces and newline character 
                line = line.strip()
                # Convert the characters in line to lowercase to avoid case mismatch
                line = line.lower()
                # Split the line into words
                words = line.split(" ")
                # get letter count
                numberOfCharacters += len(line)
                if numberOfCharacters > lettersPerPage:
                        pageNumber += 1
                # Iterate over each word in line
                for word in words:
                    # Check if the word is already in dictionary 
                    if word in d:
                        d[word].append(pageNumber)
                    else:
                        d[word] = [ pageNumber ]
            # Dict comprehension to reomve duplicate values           
            new_d = {a:list(set(b)) for a, b in d.items()}       
            # Append each new dict to a list of all dicts
            listOfAllDicts.append(new_d)
            # Empty list to store all keys
            wordKeyList = []
            # Loop over keys in new_d and append to list
            for key in new_d.keys():      
                wordKeyList.append(key)
            # Sort list and remove empty value
            wordKeyList.sort()
            wordKeyList = list(filter(None, wordKeyList))
            # Append current list of keys to the full word key list
            fullWordKeyList.append(wordKeyList)
            # Append current file name to list of file names
            fileNameList.append(filename)

def run_me(name):
    my_lock.acquire()
    textIndex(inputDirectory,outPutDirectory,lettersPerPage)
    my_lock.release()
    # Initialize list 
    wordList = []
    # Loop thru lists of lists and append each item to the flattened list
    for sublist in fullWordKeyList:
        for item in sublist:
            wordList.append(item)
    # Remove any empty keys from list
    wordList = list(filter(None, wordList))
    # Remove any duplicates from list
    wordList = list(dict.fromkeys(wordList))
    # Sort list
    wordList.sort()
    # Convert list of dictionaries into a single dict 
    finalD =  dict(ChainMap(*listOfAllDicts))
    filenamez = 'output.txt'
    # Remove any duplicates from list
    finalFileNameList = list(dict.fromkeys(fileNameList))
    # Sort list
    finalFileNameList.sort()
    completeName = os.path.join(outPutDirectory, filenamez)
    with open(completeName, 'w') as output:
        output.write("Word, " + str(', '.join(finalFileNameList)))
        output.write("\n")
        for word in wordList:
            pg_nums = finalD[word]
            output.write(word + " ")
            for pg_num in pg_nums:
                output.write(str(pg_num)+",")
            output.write("\n")

my_threads = list()
for thread_num in range(3):
    my_threads.append(threading.Thread(target=run_me, args=(thread_num,)))

my_lock = threading.Lock()

for current_thread in my_threads:
    current_thread.start()

for current_thread in my_threads:
    current_thread.join()
        
end = time.perf_counter()
total = end - start
print(str(total))


# In[ ]:


def processText(file):
    # Open the file in read mode 
            text = open(file, "r")
            # Create neccesary objects
            d = dict()
            pageNumber = 1
            numberOfCharacters = 0
            filename = os.path.basename(file)
            # Loop through each line of the file 
            for line in text:
                # Remove the leading spaces and newline character 
                line = line.strip()
                # Convert the characters in line to lowercase to avoid case mismatch
                line = line.lower()
                # Split the line into words
                words = line.split(" ")
                # get letter count
                numberOfCharacters += len(line)
                if numberOfCharacters > lettersPerPage:
                        pageNumber += 1
                # Iterate over each word in line
                for word in words:
                    # Check if the word is already in dictionary 
                    if word in d:
                        d[word].append(pageNumber)
                    else:
                        d[word] = [ pageNumber ]
            # Dict comprehension to reomve duplicate values           
            new_d = {a:list(set(b)) for a, b in d.items()}       
            # Append each new dict to a list of all dicts
            listOfAllDicts.append(new_d)
            # Empty list to store all keys
            wordKeyList = []
            # Loop over keys in new_d and append to list
            for key in new_d.keys():      
                wordKeyList.append(key)
            # Sort list and remove empty value
            wordKeyList.sort()
            wordKeyList = list(filter(None, wordKeyList))
            # Append current list of keys to the full word key list
            fullWordKeyList.append(wordKeyList)
            # Append current file name to list of file names
            fileNameList.append(filename)

