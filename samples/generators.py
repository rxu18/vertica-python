def get_generator():
    my_list = range(5)
    for i in my_list:
        yield i * i

generator = get_generator()

for i in generator:
    print(i)

#won't run as generator is empty after the first run
for i in generator:
    print(i)