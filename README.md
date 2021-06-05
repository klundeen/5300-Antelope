# 5300-Antelope

##Sprint Invierno Team: 

```Cheng Loong Kong and Nick Nguyen```

## Usage:
Steps: 
- Navigate to your cpsc5300 directory in cs1: <code>cd ~/cpsc5300</code>

- Clone the repo inside the cpsc5300 directory: <code> git clone https://github.com/klundeen/5300-Antelope.git </code> 

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
- Milestone5: Insert, Delete, Simple Queries <code>git checkout tags/Milestone5</code>
- Milestone6: BTree Index <code>git checkout tags/Milestone6</code>

## Valgrind (Linux):
To run valgrind (files must be compiled with <code>-ggdb</code>):
```sh
$ valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 data
```
Below is the valgrind command we used to check memory leaks: 
```sh
$ valgrind --leak-check=full --show-leak-kinds=all -s --suppressions=valgrind.supp ./sql5300 ~/cpsc5300/data/
```
Note that we've added suppression for the known issues with the Berkeley DB library <em>vis-à-vis</em> valgrind.

## Handoff:
- Milestone 3 should be working correctly without memory issues. Use <code>git checkout tags/Milestone3</code> to checkout our Milestone 3 code. 
- Milestone 4 should be working, but there are a few issues to look out for.
    1) We had 2 memory leak errors as of 5/10 12pm. We are working to get those resolved. 
    2) We have a segmentation fault in class DbIndex's create and drop methods  
- Milestone 5 should be working. If you get segmentation faults when dropping indices or inserting into tables then delete your database files and start again.
- Milestone 6 should be working. Similar to milestone 5, if you get unwritable pages in the cache then delete your database files and start again.

