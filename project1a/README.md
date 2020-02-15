Name: Vinay Patil
Email: vpatil3@wisc.edu

Description of the implementation
----------------------------------

1. wis-grep
   --------
   
   In this program I am first checking for the case when no searh term is provided if it's the case then I show the appropriate error message and exit with 1.
   Then I check if just the search term is provided and not any file names to search for, then I read the input line by line from the stdin untill an interrupt is encountered.
   Finally if the file names are provided as well then, before search through a file for the term, I check if its a valid file and it exists and display appropriate error message otherwise and exit. If the file name is valid I open the file and read each line and search it for the search term and print the line if it has a match.
   For the empty string case I am printing out all the lines

2. wis-tar
   -------

   I first check if fewer than two arguments are supplied to the program, if it's the case then I show appropriate error message and exit with 1.
   Then if it's a appropriate input, I loop through each of the files and check if the file exit and can be opened, if not then I show the relevant error message, if it can be opened I read its name from the argument and store it as per the requirement of null characters, its size and write the relevant information to the tar file that will be created or overwritten before the start of this loop.
   Once all the files have been processed successfully, I close the opened files and exit with 0.

3. wis-untar
   ---------

   I first check to see if the no of arguments are too less or too many, if it's the case then I show the appropriate error message and exit with the status 1.
   Then I check if the tar file given is valid and can be opened, if it's not then I show the appropriate error message and exit with the status 1.
   Finally I open the tar file and read the contents (filename, filesize, filecontent) and create a new file with those deatils in the same folder. Here the filecontent is read in a batch size of 1024 to handle file content which is too big to handle by fread. I keep creating such files till I reach the end of the file in the opened tar file.


Thank you.
