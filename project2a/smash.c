#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include <ctype.h>

// Path DataStructure and it's functionalities
struct Path {
  char *path;
  struct Path *next;
};

void errorMessage(){
  char error_message[30] = "An error has occurred\n";
  write(STDERR_FILENO, error_message, strlen(error_message)); 
}

char *line = NULL;
size_t size = 0;

struct Path *path_head = NULL;

void push(struct Path **head_ref, char *new_data) {
  struct Path *new_node = (struct Path *)malloc(sizeof(struct Path));
  new_node->path = new_data;
  new_node->next = (*head_ref);
  (*head_ref) = new_node;
}

void deleteNode(struct Path **head_ref, char *key) {
  struct Path *temp = *head_ref, *prev;
  int isheadkey = 0, bodyhaskey = 0;
  if (temp != NULL && strcmp(temp->path, key) == 0) {
    *head_ref = temp->next;
    free(temp);
    temp = *head_ref;
    isheadkey = 1;
  }

  while (temp != NULL){
    while(temp != NULL && strcmp(temp->path, key) != 0) {
      prev = temp;
      temp = temp->next;
    }

    if (temp == NULL)
    {
      if(isheadkey==0 && bodyhaskey==0){
        errorMessage();
      }
      return;
    }
    
    if(prev == NULL){
      *head_ref = temp->next;
      free(temp);
      temp = *head_ref;
    }
    else{
      prev->next = temp->next;
      free(temp);
      temp = prev->next;
    }
    bodyhaskey = 1;
  }
  if(isheadkey==0 && bodyhaskey==0){
    errorMessage();
  }
  return;
}

/* Function to delete the entire linked list */
void deleteList(struct Path **head_ref) {
  /* deref head_ref to get the real head */
  struct Path *current = *head_ref;
  struct Path *next;

  while (current != NULL) {
    next = current->next;
    free(current);
    current = next;
  }

  /* deref head_ref to affect the real head back
     in the caller. */
  *head_ref = NULL;
}

void printList(struct Path *n) {
  while (n != NULL) {
    printf("%s\n", n->path);
    n = n->next;
  }
}

// End of Path DataStructure and it's functionalities

// Command Datastructure and it's functionalities
struct Command {
  char **arg_list;
  char *redirect_filename;
  char *call_path;
  int arg_count;
  struct Command *next;
};

void push_command(struct Command **head_ref, char *file, char **args, char* call_path,
                  int argc) {

  struct Command *new_node = (struct Command *)malloc(sizeof(struct Command));

  struct Command *last = *head_ref;

  new_node->redirect_filename = file;

  new_node->call_path = call_path;

  new_node->arg_list = malloc((argc+1) * sizeof(char *));

  for (int i = 0; i < argc; i++) {
    new_node->arg_list[i] = malloc(strlen(args[i]) * sizeof(char));
    strcpy(new_node->arg_list[i], args[i]);
  }

  new_node->arg_list[argc] = NULL;


  new_node->arg_count = argc;

  if (*head_ref == NULL) {
    *head_ref = new_node;
    return;
  }

  while (last->next != NULL)
    last = last->next;

  last->next = new_node;
  return;
}

void printCommands(struct Command *node) {
  while (node != NULL) {
    printf("File name is %s\n ", node->redirect_filename);
    printf("%s\n", "Printing args");
    for (int i = 0; i < node->arg_count; i++) {
      printf("%s\n", node->arg_list[i]);
    }

    node = node->next;
  }
}

// End of Command Datastructure and it's functionalities

struct Parsed {
  struct Command *command;
  struct Parsed *next;
};

void push_parsed(struct Parsed **head_ref, struct Command *node) {

  struct Parsed *new_node = (struct Parsed *)malloc(sizeof(struct Parsed));

  struct Parsed *last = *head_ref;

  new_node->command = node;

  if (*head_ref == NULL) {
    *head_ref = new_node;
    return;
  }

  while (last->next != NULL)
    last = last->next;

  last->next = new_node;
  return;
}

void printParsed(struct Parsed *node) {
  while (node != NULL) {
    printf("%s\n", "Showing next parsed");
    printCommands(node->command);

    node = node->next;
  }
}

int isempty(char *s)
{
  while (*s) {
    if (!isspace(*s))
      return 0;
    s++;
  }
  return 1;
}

void helper() {

      char *main_str = strdup(line);

      char *multi_command;

      struct Parsed *parsed_head = NULL;

      while ((multi_command = strsep(&main_str, ";")) != NULL) {

        // Ignoring Empty commands - Not an error
        if (*multi_command == '\0') {
          continue;
        }

        char *dup_multi_command = strdup(multi_command);

        char *parallel_command;

        struct Command *command_head = NULL;

        while ((parallel_command = strsep(&dup_multi_command, "&")) != NULL) {
          // Ignoring Empty commands - Not an error
          if (*parallel_command == '\0') {
            continue;
          }

          char *dup_parallel_command = strdup(parallel_command);

          char *redirect_command;

          int redirect_index = -1;

          char *redirect_components[1024];

          char *redirect_file = NULL;

          // Default command if redirect_index=0
          char *cmd_arg = strdup(parallel_command);

          while ((redirect_command = strsep(&dup_parallel_command, ">")) !=
                 NULL) {
            if (redirect_index == -1 && dup_parallel_command == NULL) {
              break;
            }

            redirect_index = redirect_index + 1;
            redirect_components[redirect_index] = redirect_command;

            // Incorrect Redirect Command
            if (redirect_index > 1) {
              errorMessage();
              return;
            }
          }

          // If redirect has been detected
          if (redirect_index == 1) {

            // Error if either left or right commands are empty
            if (isempty(redirect_components[0]) == 1 ||
                isempty(redirect_components[1]) == 1) {
              errorMessage();
              return;
            }
            char *redirect_file_temp =
                malloc(strlen(redirect_components[1]) * sizeof(char *));
            strcpy(redirect_file_temp, redirect_components[1]);

            char *inner;

            int inner_index = -1;

            cmd_arg = malloc(strlen(redirect_components[0]) * sizeof(char *));
            strcpy(cmd_arg, redirect_components[0]);

            // Check to see if multiple files given as redirect arguments
            while ((inner = strsep(&redirect_file_temp, " ")) != NULL) {
              if (*inner == '\0') {
                continue;
              }
              inner_index = inner_index + 1;

              // Incorrect Redirect File Specified
              if (inner_index > 0) {
                errorMessage();
                return;
              }

              redirect_file = malloc(strlen(inner) * sizeof(char *));
              strcpy(redirect_file, inner);
            }
          }

          char *each;

          char *components[1024];
          int index = -1;

          char* builtin_args[3];

          while ((each = strsep(&cmd_arg, " ")) != NULL) {
            if (*each == '\0') {
              continue;
            }
            index = index + 1;
            components[index] = each;
          }

          if (index == -1) {
            // Empty Input - Continue
            continue;
          }

          if (strcmp(components[0], "exit") == 0) {
	    if (index > 0) {
              errorMessage();
              return;
            }
            builtin_args[0] = "exit";
            push_command(&command_head, NULL, builtin_args,NULL, 1);
          }

          else if (strcmp(components[0], "cd") == 0) {
            // Invalid no of arguments
            if (index != 1) {
              errorMessage();
              return;
            }

            if(access(components[1],F_OK|X_OK)!=0){
              errorMessage();
              return;
            }

            builtin_args[0] = "cd";

            builtin_args[1] = components[1];

            push_command(&command_head, NULL, builtin_args,NULL, 2);
          }

          else if (strcmp(components[0], "path") == 0) {
            // Insufficient args
            if (index == 0) {
              errorMessage();
              return;
            }

            builtin_args[0] = "path";

            if (strcmp(components[1], "add") == 0) {

              // Less/Extra args given
              if (index != 2) {
                errorMessage();
                return;
              }

              builtin_args[1] = "add";

              builtin_args[2] = components[2];

              push_command(&command_head, NULL, builtin_args,NULL, 3);
            }

            else if (strcmp(components[1], "remove") == 0) {
              // Less/Extra args given
              if (index != 2) {
                errorMessage();
                return;
              }

              builtin_args[1] = "remove";

              builtin_args[2] = components[2];

              push_command(&command_head, NULL, builtin_args,NULL, 3);
            }

            else if (strcmp(components[1], "clear") == 0) {
              // Less/Extra args given
              if (index != 1) {
                errorMessage();
                return;
              }

              builtin_args[1] = "clear";

              push_command(&command_head, NULL, builtin_args,NULL, 2);
            }
          }

          else {
            struct Path *current = path_head;
            struct Path *next;

            char *call_path = NULL;

            while (current != NULL) {
              char *str1 = current->path;
              char *str2 = components[0];

              char result[strlen(str1) + strlen(str2) + 1];
              result[0] = 0;

              strcat(result, str1);
              strcat(result, "/");
              strcat(result, str2);

              if (access(result, X_OK) == 0) {
                call_path =
                    malloc(strlen(str1) + strlen(str2) + 1 * sizeof(char *));
                strcpy(call_path, result);
                break;
              }
              next = current->next;
              current = next;
            }

            if (current == NULL) {
              errorMessage();
              return;
            }

            char *arg_list[index + 2];

            for (int i = 0; i <= index; i++) {
              arg_list[i] = components[i];
            }

            arg_list[index + 1] = NULL;

            push_command(&command_head, redirect_file, arg_list,call_path, index + 1);
          }

        } // End of & Loop

        push_parsed(&parsed_head, command_head);

      } // End of ; Loop
      //printParsed(parsed_head);

      while (parsed_head != NULL) {
        struct Command *node = parsed_head->command;
        int status=0, wait_id;

        while (node != NULL) {

          if(strcmp(node->arg_list[0],"exit")==0){
            exit(0);
          }
          else if(strcmp(node->arg_list[0],"cd")==0){
            int err = chdir(node->arg_list[1]);
            if (err < 0) {
              errorMessage();
              return;
            }
            node = node->next;
            continue;
          }
          else if(strcmp(node->arg_list[0],"path")==0){
            if (strcmp(node->arg_list[1], "add") == 0){
              push(&path_head, node->arg_list[2]);
            }
            else if (strcmp(node->arg_list[1], "remove") == 0){
              deleteNode(&path_head, node->arg_list[2]);
            }
            else if (strcmp(node->arg_list[1], "clear") == 0){
              deleteList(&path_head);
            }
           // printList(path_head);
            node = node->next;
            continue;
          }

          else{
            char *filename = node->redirect_filename;
            
            int pid = fork();
            if (pid == -1) {
              errorMessage();
              return;
            } else if (pid == 0) {
              // Child
              if (filename != NULL) {
                FILE *file = fopen(filename, "w");

		if(file==NULL){
	          errorMessage();
		  return;
		}

                int fd = fileno(file);
                dup2(fd, 1);
                dup2(fd, 2);
              }
              
              execv(node->call_path, node->arg_list);
              errorMessage();
              return;
            }
            node = node->next;
          }
        }

        // Wait for all the forked processes to finish
        while ((wait_id = wait(&status)) > 0);

        parsed_head = parsed_head->next;
      }

}


int main(int argc, char *argv[]) {
  
  FILE *FP;
  
  ssize_t total;

  path_head = (struct Path *)malloc(sizeof(struct Path));

  path_head->path = "/bin";
  path_head->next = NULL;

  if (argc == 1) {

    while (1) {

      printf("%s", "smash> ");

      fflush(stdout);

      total = getline(&line, &size, stdin);
      if (feof(stdin)){
	printf("\n");
        exit(0);
      }
      if (line[total - 1] == '\n') {
        line[total - 1] = '\0';
      }
      helper();
    }
  }

  else if(argc == 2){
    FP = fopen(argv[1], "r");
    if (FP == NULL)
    {
      exit(1);
    }

    while ((total = getline(&line, &size, FP)) != -1){
      //Checking for EOF input
      if (feof(FP)){
        exit(0);
      }
      if (line[total - 1] == '\n') {
        line[total - 1] = '\0';
      }
      helper();
    }
  }

  else{
    exit(1);
  }
}
