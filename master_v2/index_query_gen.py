index_commands = []

for i in range(1, 100):
    command = f"CREATE INDEX idx{i}  USING BTREE ON tbl(a{i});"
    index_commands.append(command)

for cmd in index_commands:
    print(cmd)