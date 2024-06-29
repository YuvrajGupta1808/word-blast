/**************************************************************
 * Class::  CSC-415-0# Summer 2024
 * Name:: Yuvraj Gupta
 * Student ID:: 922933190
 * GitHub-Name:: YuvrajGupta1808
 * Project:: Assignment 4 – Word Blast
 *
 * File:: Gupta_Yuvraj_HW4_main.c
 *
 * Description:: This program reads War and Peace, counts and 
 * tallies words of 6 or more characters using multiple threads.
 **************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <ctype.h>

#define MAX_WORD_LEN 256
#define TOP_N 10

typedef struct {
    char word[MAX_WORD_LEN];
    int count;
} WordCount;

typedef struct {
    int threadId;
    const char *filename;
    int start;
    int end;
} ThreadArgs;

WordCount *wordCounts;
int wordCountSize = 0;
pthread_mutex_t wordCountMutex;
int threadCount;

char *delim = " \"\'.“”‘’?:;-,—*($%)! \t\n\x0A\r";

void *process_chunk(void *args);
void add_word(const char *word);
void print_top_n();
int compare_words(const void *a, const void *b);

void *process_chunk(void *args) {
    ThreadArgs *threadArgs = (ThreadArgs *)args;
    const char *filename = threadArgs->filename;
    int start = threadArgs->start;
    int end = threadArgs->end;

    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        perror("Failed to open file");
        pthread_exit(NULL);
    }

    // Read the chunk
    int chunkSize = end - start;
    char *buffer = malloc(chunkSize);  // Allocate buffer for chunk
    if (buffer == NULL) {
        perror("Failed to allocate buffer");
        close(fd);
        pthread_exit(NULL);
    }

    // Read chunkSize bytes from the file starting at 'start'
    int bytesRead = pread(fd, buffer, chunkSize, start);
    if (bytesRead == -1) {
        perror("Failed to read file chunk");
        free(buffer);
        close(fd);
        pthread_exit(NULL);
    }
    close(fd);

    // Ensure we start at a word boundary if this is not the first chunk
    if (start != 0) {
        char *adjustedStart = buffer;
        while (*adjustedStart && !strchr(delim, *adjustedStart)) {
            adjustedStart++;
        }
        if (*adjustedStart) {
            adjustedStart++;
        }
        memmove(buffer, adjustedStart, bytesRead - (adjustedStart - buffer));
    }

    // Tokenize and process words using strtok_r
    char *saveptr;
    char *token = strtok_r(buffer, delim, &saveptr);
    while (token != NULL) {
        if (strlen(token) >= 6) {
            add_word(token);
        }
        token = strtok_r(NULL, delim, &saveptr);
    }

    free(buffer);
    free(threadArgs);
    pthread_exit(NULL);
}

void add_word(const char *word) {
    int foundIndex = -1;

    // Check if the word already exists in the array (read-only operation)
    for (int i = 0; i < wordCountSize; i++) {
        if (strcasecmp(wordCounts[i].word, word) == 0) {
            foundIndex = i;
            break;
        }
    }

    pthread_mutex_lock(&wordCountMutex);  // Lock mutex for thread synchronization

    if (foundIndex != -1) {
        // Word found, update the count
        wordCounts[foundIndex].count++;
    } else {
        // If the word is new, add it to the array
        strcpy(wordCounts[wordCountSize].word, word);
        wordCounts[wordCountSize].count = 1;
        wordCountSize++;
    }

    pthread_mutex_unlock(&wordCountMutex);  // Unlock mutex
}

// Comparison function for sorting words by count in descending order
int compare_words(const void *a, const void *b) {
    return ((WordCount*)b)->count - ((WordCount*)a)->count;
}

// Function to print the top N words
void print_top_n() {
    qsort(wordCounts, wordCountSize, sizeof(WordCount), compare_words);
    // Sort words by count

    printf("Printing top %d words 6 characters or more.\n", TOP_N);
    for (int i = 0; i < TOP_N && i < wordCountSize; i++) {
        printf("Number %d is %s with a count of %d\n", i + 1,
         wordCounts[i].word, wordCounts[i].count);
    }
}

int main (int argc, char *argv[]) {
    //**************************************************************
    // DO NOT CHANGE THIS BLOCK
    //Time stamp start
    struct timespec startTime;
    struct timespec endTime;

    clock_gettime(CLOCK_REALTIME, &startTime);
    //**************************************************************

    if (argc != 3) {
        fprintf(stderr, "Usage: %s <filename> <thread_count>\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    
    const char *filename = argv[1];
    threadCount = atoi(argv[2]);
    
    // Initialize mutex
    pthread_mutex_init(&wordCountMutex, NULL);
    
    // Get file size
    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        perror("Failed to open file");
        exit(EXIT_FAILURE);
    }
    int fileSize = lseek(fd, 0, SEEK_END);
    if (fileSize == -1) {
        perror("Failed to get file size");
        close(fd);
        exit(EXIT_FAILURE);
    }
    close(fd);
    
    // Allocate memory for word counts
    wordCounts = malloc(fileSize / 6 * sizeof(WordCount));  
    // Rough estimate for initial allocation
    if (wordCounts == NULL) {
        perror("Failed to allocate wordCounts");
        exit(EXIT_FAILURE);
    }

    // Create threads
    pthread_t threads[threadCount];
    for (int i = 0; i < threadCount; i++) {
        // Calculate the start and end position for each chunk
        int start = i * (fileSize / threadCount);
        int end = (i == threadCount - 1) ? fileSize : (i + 1) * (fileSize / threadCount);

        // Ensure that the chunk starts at a word boundary
        if (start != 0) {
            int fd = open(filename, O_RDONLY);
            if (fd == -1) {
                perror("Failed to open file");
                exit(EXIT_FAILURE);
            }
            lseek(fd, start, SEEK_SET);
            char c;
            while (read(fd, &c, 1) == 1 && !strchr(delim, c)) {
                start++;
            }
            close(fd);
        }

        // Allocate and initialize thread arguments
        ThreadArgs *threadArgs = malloc(sizeof(ThreadArgs));
        if (threadArgs == NULL) {
            perror("Failed to allocate threadArgs");
            exit(EXIT_FAILURE);
        }
        threadArgs->threadId = i;
        threadArgs->filename = filename;
        threadArgs->start = start;
        threadArgs->end = end;

        // Create the thread
        if (pthread_create(&threads[i], NULL, process_chunk, (void *)threadArgs) != 0) {
            perror("Failed to create thread");
            exit(EXIT_FAILURE);
        }
    }
    
    // Wait for threads to finish
    for (int i = 0; i < threadCount; i++) {
        if (pthread_join(threads[i], NULL) != 0) {
            perror("Failed to join thread");
            exit(EXIT_FAILURE);
        }
    }
    
    // Print the header before printing the top N words
    printf("Word Frequency Count on %s with %d threads\n\n", filename, threadCount);
    
    // Print top N words
    print_top_n();

    //**************************************************************
    // DO NOT CHANGE THIS BLOCK
    //Clock output
    clock_gettime(CLOCK_REALTIME, &endTime);
    time_t sec = endTime.tv_sec - startTime.tv_sec;
    long n_sec = endTime.tv_nsec - startTime.tv_nsec;
    if (endTime.tv_nsec < startTime.tv_nsec) {
        --sec;
        n_sec = n_sec + 1000000000L;
    }

    printf("Total Time was %ld.%09ld seconds\n", sec, n_sec);
    //**************************************************************

    // Clean up
    free(wordCounts);
    pthread_mutex_destroy(&wordCountMutex);

    return 0;
}
