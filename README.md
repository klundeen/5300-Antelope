# 5300-Antelope

##Sprint Otono Team: 

```Jara Lindsay, Bryn Lasher, and Lili Hao```

## Usage:
Steps: 
- Navigate to your cpsc5300 directory in cs1: <code>cd ~/cpsc5300</code>

- Clone teh repo inside the cpsc5300 directory: <code> git clone https://github.com/klundeen/5300-Antelope.git </code> 

- Navigate to the cloned repository: <code>cd 5300-Antelope</code>

- To checkout tags: <code>git checkout tags/Milestone(1/2/3/4)</code>

- Make the program: <code>make</code>

- Clean the solution (if you need to): <code>make clean</code>

- To run the program: <code>$ ./sql5300 ~/cpsc5300/data</code>

- To exit the program:  <code>quit</code>

## Tags:
- Milestone1: SQL statement engine <code>git checkout tags/Milestone1</code>
- Milestone2: Rudimentary Storage Engine <code>git checkout tags/Milestone2</code>
- Milestone3: Schema Storage <code>git checkout tags/Milestone3</code>
- Milestone4: Indexing <code>git checkout tags/Milestone4</code>
## Unit Tests:
There are some tests for SlottedPage and HeapTable. They can be invoked from the <code>SQL</code> prompt:
```sql
SQL> test
```
Be aware that failed tests may leave garbage Berkeley DB files lingering in your data directory. If you don't care about any data in there, you are advised to just delete them all after a failed test.
```sh
$ rm -f data/*
```

## Valgrind (Linux):
To run valgrind (files must be compiled with <code>-ggdb</code>):
```sh
$ valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 data
```
Note that we've added suppression for the known issues with the Berkeley DB library <em>vis-à-vis</em> valgrind.

## Handoff:
- In Progress
 
## Video:
- Link: 