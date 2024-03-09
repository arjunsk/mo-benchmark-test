index_commands = []

for i in range(1, 100):  # Loop from 1 to 99
    command = f"CREATE INDEX idx{i}  USING BTREE ON tbl(a{i});"
    index_commands.append(command)

# Now, index_commands contains all the SQL commands to create the indexes

# You can print them out or execute them against your database
for cmd in index_commands:
    print(cmd)