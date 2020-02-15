Name: Vinay Patil

Email: vpatil3@wisc.edu


The c file smash.c has my implementation for custom shell featuring all the requested functionalities.

Implementation Logic: 

The path for the shell is maintained in a dynamic linked list which supports addition and removal of duplicate paths. 

I read input's line by line from either the stdin or from a batch file, then parse the input line to see if it's well formed(The precedence is ;, &, >, cmd). 

If there are any error's in the input line then I print the standard error message. 

If the line is well formed, then I write the tokens to a dynamic nested linked list in a specific format. 

The outer linked list is to capture the functionality of multiple commands where as the inner linked list is used to handle parallel exection.

Once the input is parsed for validity and nested linked list is created, I go over the linked list in sequence executing the commands either in parallel or in sequence depending on the requirement.

Built in commands are always executed by the parent process whereas all the other commands are being executed by the forked child processes.

Thanks.
