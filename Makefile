# Name of the final binary
TARGET = cvmrdma_manager

# Compiler and flags
CC = gcc
CFLAGS = -Wall -Wextra -g

# Source files
SRCS = $(wildcard *.c)
OBJS = $(SRCS:.c=.o)

# Default rule
all: $(TARGET)

# Link object files to create the binary
$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^

# Compile .c files into .o files
%.o: %.c %.h
	$(CC) $(CFLAGS) -c $< -o $@

# Clean generated files
clean:
	rm -f $(OBJS) $(TARGET)
