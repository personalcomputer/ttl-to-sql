#!/usr/bin/env python
import sys
import os
import sqlite3

#Note: this only parses the simplest of .ttl files (not even the entire N-Triples standard is supported). No prefixes supported at all.
#http://www.w3.org/TR/n-triples/
#http://www.w3.org/TeamSubmission/turtle/

default_table = 'triples'

usage = 'Usage: '+sys.argv[0]+''' TTL_FILE SQLITE3_FILE [OPTION]...
Import N-Triple (Turtle subset) file into SQL database.

  --table      Database table name (default: '''+default_table+''')

https://github.com/personalcomputer/ttl-to-sql'''

create_indices = True

ttl_encoding = 'utf-8'

entries_per_transaction = 10000

class ParseError(Exception):
  def __init__(self, msg):
    self.msg = msg
  def __repr__():
    return "%s(%r)" % (self.__class__, self.__dict__)
  def __str__(self):
    return self.msg

def parse_entry(entry):
  #returns (s,p,o) tuple

  if entry[0] == '@':
    raise ParseError('ttl file uses advanced Turtle features. This converter only supports the very simplest of ttl files. (No prefixes supported at all)')

  subject_s = ''
  predicate_s = ''
  object_s = ''

  subject_start = None
  predicate_start = None
  object_start = None

  #_Extremely ugly_ manual grammar parsing. Not using regex or parser generators for speed. (This tool was written for a database with 6 million entries.)
  #Regardless, custom parsers are unacceptably ugly and not maintainable, so if even a tiny new feature is supported this will be converted to use a parser generator.
  #If I need more speed I will port the entire project to C++. It is quite small so porting is not an issue. (SQLITE3 certainly is also a large bottleneck, but parsing is even worse. Logic could reasonably be around ~30x+ faster than python, and the sqlite3 behavior can be massively improved with minimal effort. See the readme.md.)
  for i,c in enumerate(entry):
    if not subject_s:
      if subject_start == None: #
        #first char of subject
        if c != '<':
          raise ParseError('Unexpected '+str(c)+' at char '+str(i)+' while parsing subject')
        subject_start = i
        continue
      elif c == '>':
        subject_s = entry[subject_start:i+1]
        continue
      continue
    elif not predicate_s:
      if predicate_start == None:
        #first char of predicate
        if c == ' ':
          continue
        if c != '<':
          raise ParseError('Unexpected '+str(c)+' at char '+str(i)+' while parsing predicate')
        predicate_start = i
        continue
      elif c == '>':
        predicate_s = entry[predicate_start:i+1]
        continue
      continue
    elif not object_s:
      if object_start == None:
        #first char of object
        if c == ' ':
          continue
        if c not in ['<','"']:
          raise ParseError('Unexpected '+str(c)+' at char '+str(i)+' while parsing object')
        object_start = i
        continue
      elif c in ['>','"']:
        object_s = entry[object_start:i+1]
        #continue
        break
      continue
    #elif c not in ['.',' ','\n']:
    #  raise ParseError('Unexpected '+str(c)+' at char '+str(i))

  return subject_s.decode(ttl_encoding), predicate_s.decode(ttl_encoding), object_s.decode(ttl_encoding)

def create_index(conn, c, table_name, column):
  print('Creating '+column+' index...')
  c.execute('CREATE INDEX '+column+'_index ON '+table_name+'('+column+')')
  conn.commit()

def table_exists(c, table_name):
  c.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="'+table_name+'"')
  return c.fetchone() != None

def main():
  args = sys.argv[1:]

  ttl_filename = ''
  database_filename = ''
  table_name = default_table
  try:
    ttl_filename = args[0]
  except IndexError:
    print('No ttl file supplied.\n'+usage)
    sys.exit(1)
  try:
    database_filename = args[1]
  except IndexError:
    print('No sqlite3 database supplied.\n'+usage)
    sys.exit(1)
  if(len(args)>3):
    #Use python argparse if any more options are added
    if args[2] == '--table':
      assert(len(args)>=4)
      table_name = args[3]
      assert(table_name.strip() != '')


  if not os.path.exists(ttl_filename):
    print('Supplied ttl file "'+ttl_filename+'" does not exist.\n'+usage)
    sys.exit(1)

  ttl = open(ttl_filename)
  ttl_filesize = os.stat(ttl_filename).st_size

  #prompt to overwrite
  is_new_database = True
  if os.path.exists(database_filename):
    overwrite = raw_input('Overwrite '+database_filename+'? (y/N): ')
    if overwrite in ['y','Y','yes']:
      os.remove(database_filename)
    else:
      print('Inserting into existing database.')
      is_new_database = False

  #open & configure db
  conn = sqlite3.connect(database_filename)
  c = conn.cursor()
  is_new_table = not table_exists(c, table_name)
  c.execute('CREATE TABLE IF NOT EXISTS '+table_name+' (subject TEXT, predicate TEXT, object TEXT)')
  if is_new_database:
    c.execute('PRAGMA synchronous = OFF')
    c.execute('PRAGMA journal_mode = MEMORY')

  line_number = 0
  uncommitted_entries_accumulator = 0 #accumulator style to allow tracking sql statements not raw entries (matters in the case of dropped invalid entries).
  error_accumulator = 0

  for line_number,entry in enumerate(ttl):
    if entry[0] == '#':
      continue

    try:
      s,p,o = parse_entry(entry)
      c.execute('INSERT INTO '+table_name+' VALUES (?,?,?)', (s, p, o))
      uncommitted_entries_accumulator += 1

    except ParseError as e:
      print('\n'+str(e))
      print('failed on line #'+str(line_number)+': '+entry)
      error_accumulator += 1
      #sys.exit(1)

    if uncommitted_entries_accumulator >= entries_per_transaction:
      conn.commit()
      uncommitted_entries_accumulator = 0

      #also report progress
      percent_progress = (ttl.tell()/float(ttl_filesize))*100
      percent_error = (error_accumulator/float(line_number))*100
      sys.stdout.write('Conversion Progress: {0} entries, progress: {1:.2f}% ({2:.6f}% errors)\r'.format(line_number, percent_progress, percent_error))
      sys.stdout.flush()

  sys.stdout.write('\n') #next line, no more progress reports
  conn.commit() #in case they ctrl+c the first index and there is an uncommitted transaction (n < entries_per_transaction)

  #indices
  if is_new_table and create_indices:
    create_index(conn, c, table_name, 'subject')
    create_index(conn, c, table_name, 'predicate')
    create_index(conn, c, table_name, 'object')

  conn.close()

  ttl.close()

if __name__ == '__main__':
  main()
