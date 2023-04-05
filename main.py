# import necessary libraries for generating lists of
#   strings of English alphabet characters.
import string
import numpy
# For declaring variables as non-modifiable
from typing import Final
# For measuring time elapsed for each sorting algorithm
import time
# For deep copying lists
import copy
# All multiprocessing imports are for parallelizing quicksort
import multiprocessing

def serial_quicksort(a_list):
    # Assert that no parameter can be "None"
    assert a_list is not None
    # Recursively partition array into halves and sort
    #    each half until we're sorting lists each of
    #    length 0, then unwind the recursion and merge
    #    the sorted halves together while including in
    #    the middle the partitioning element used to
    #    partition the array recursively.
    if (len(a_list) > 0):
        # Use middle element for partitioning to avoid O(n^2)
        #    runtime especially in cases where the list may
        #    already be sorted
        partitioning_element = a_list.pop((len(a_list) - 1) // 2)
        # List containing all elements no larger than "partitioning_element"
        #    from "a_list".
        no_larger_than_list = []
        # List containing all elements larger than "partitioning_element"
        #    from "a_list".
        larger_than_list = []
        # partition "a_list" using "partitioning_element"
        partition(a_list=a_list,
                     no_larger_than_list=no_larger_than_list,
                     larger_than_list=larger_than_list,
                                partitioner=partitioning_element)
        # Recursively sort the two lists
        serial_quicksort(no_larger_than_list)
        serial_quicksort(larger_than_list)
        # Combine the two recursively sorted lists with
        #   the partitioning element in the proper order
        a_list.extend(no_larger_than_list)
        a_list.append(partitioning_element)
        a_list.extend(larger_than_list)

# Parameter details:
# - a_list: list to be partitioned into two parts: entries greater
#           than "partitioner" and entries less than "partitioner";
#           note that "a_list" WILL BE EMPTY when this function
#           returns
# - no_larger_than_list: When this function returns, will contain all
#                        elements from "a_list" no larger than "partitioner"
# - larger_than_list:  When this function returns, will contain all
#                      elements from "a_list" larger than "partitioner"
# - partitioner: The value used to partition the list
def partition(a_list, no_larger_than_list, larger_than_list, partitioner):
    # Assert that no parameter can be "None"
    assert a_list is not None
    assert no_larger_than_list is not None
    assert larger_than_list is not None
    assert partitioner is not None

    # Make sure "no_larger_than_list" and "larger_than_list" are empty lists
    no_larger_than_list.clear()
    larger_than_list.clear()

    # Remove each element from "a_list", and add the element to either
    # "no_larger_than_list" or "larger_than_list" depending on whether
    # element is larger than, equal to, or smaller than "partitioner"
    while (len(a_list) > 0):
        element = a_list.pop()
        if (element > partitioner):
            larger_than_list.append(element)
        else:
            no_larger_than_list.append(element)

def parallel_quicksort(a_list, sending_socket, current_processes_count,
                                                    MAX_PROCESSES_COUNT):
    # Assert that no parameter can be "None"
    assert a_list is not None
    assert sending_socket is not None
    assert current_processes_count is not None
    assert MAX_PROCESSES_COUNT is not None

    # Recursively partition the list and sort each partition until each
    #     partition is a list of length zero (i.e. the base case has been
    #     reached). Then unwind the recursion and combine the now sorted
    #     partitions into a overall sorted list.
    if (len(a_list) > 0):
        # If we've spawned enough processes recursively for
        #    sorting in parallel, switch over to sorting each
        #    partition of the list via the serial version of
        #    quicksort.
        if (current_processes_count >= MAX_PROCESSES_COUNT):
            serial_quicksort(a_list=a_list)
            # send sorted result to parent process
            sending_socket.send(a_list)
            # close socket as we're done with inter-process communication
            sending_socket.close()
        # Otherwise, partition list using a middle partition element
        #     as in the serial version of quicksort, but here we
        #     send each resulting partition of the list to a process
        #     we spawn that calls this function recursively to
        #     sorting each resulting partition.
        else:
            # Use middle element for partitioning to avoid O(n^2)
            #    runtime especially in cases where the list may
            #    already be sorted
            partitioning_element = a_list.pop((len(a_list) - 1) // 2)
            # List containing all elements no larger than "partitioning_element"
            #    from "a_list".
            no_larger_than_list = []
            # List containing all elements larger than "partitioning_element"
            #    from "a_list".
            larger_than_list = []
            # partition "a_list" using "partitioning_element"
            partition(a_list=a_list, no_larger_than_list=no_larger_than_list,
                           larger_than_list=larger_than_list, partitioner=partitioning_element)
            # Create Pipes for child processes recursively calling
            #     this function to send their sorted results over to
            #     the parent process
            receive_sock_no_larger_than_proc, send_sock_no_larger_than_proc = \
                                                    multiprocessing.Pipe(duplex=False)
            receive_sock_larger_than_proc, send_sock_larger_than_proc = \
                                                    multiprocessing.Pipe(duplex=False)
            # If each process spawns 2 processes, then at any given level of recursion
            #     we're spawning 2^i processes, where "i" equals how many times
            #     we've called this function recursively. So given n levels of
            #     recursion we have a total of 2^(n+1)-1 processes (including
            #     the root processes) at any given point, which is equivalent to
            #     saying p = 2p + 1 where "p" equals the total number of processes
            #     we've spawned thus far (not that the "=" is assignment and NOT equality!).
            new_process_count = 2 * current_processes_count + 1
            # Create processes which each recursively calls this function for sorting
            #     a partition of "a_list".
            no_larger_than_proc = multiprocessing.Process(target=parallel_quicksort,
                                                            args=(no_larger_than_list,
                                                                  send_sock_no_larger_than_proc,
                                                                  new_process_count,
                                                                  MAX_PROCESSES_COUNT))
            larger_than_proc = multiprocessing.Process(target=parallel_quicksort,
                                                            args=(larger_than_list,
                                                                  send_sock_larger_than_proc,
                                                                  new_process_count,
                                                                  MAX_PROCESSES_COUNT))
            # Launch processes spawned to have each process recursively sort a partition
            #     of the list and send the sorted result back over to this parent process.
            no_larger_than_proc.start()
            larger_than_proc.start()
            # Fetch the sorted list partitions from the child processes
            no_larger_than_list = receive_sock_no_larger_than_proc.recv()
            larger_than_list = receive_sock_larger_than_proc.recv()
            # Combine the sorted partitions along with the middle partitioning element
            #     and send the combined result (i.e. the final sorted list/partition)
            #     over to the parent process.
            a_list.extend(no_larger_than_list)
            a_list.append(partitioning_element)
            a_list.extend(larger_than_list)
            sending_socket.send(a_list)
            # Inter-processes communication finished; close the socket used
            #     to send the sorted partition over to the parent process.
            #     Because each child process spawned above received a sending
            #     socket and recursively called this function using that
            #     sending socket, the following "close()" method also closes
            #     the inter-process communication pipes used to retrieve the
            #     sorted partitions from each child process.
            sending_socket.close()
            # Wait for each process to finish transmitting the partition it
            #     has sorted to the parent process and make sure each process
            #     has released its resources before exiting this function.
            no_larger_than_proc.join()
            larger_than_proc.join()
            no_larger_than_proc.close()
            larger_than_proc.close()
    # To avoid a deadlock on the base case when number of
    #    processes spawned recursively is less than MAX_PROCESSES_COUNT,
    #    send over the empty list to the parent process and
    #    then close the inter-process communication pipe.
    else:
        sending_socket.send(a_list)
        sending_socket.close()
# END IMPORTS SECTION #


# GLOBAL VARIABLES SECTION #

# Length of each randomly generated string
LENGTH_OF_STRING: Final = 20
# Length of randomly generated string list
LENGTH_OF_STRING_LIST: Final = 2000000


# END GLOBAL VARIABLES SECTION #


# BEGIN FUNCTIONS DECLARATIONS SECTION #

# Main program comparing time took to execute serial version of
#    quicksort vs time took to execute parallel version of quicksort.
def main():
    print("\nInitializing list copies to be sorted (this may take some time)...")

    # randomly generated matrix of characters to be converted to a list of
    #     strings to be sorted
    char_matrix = numpy.random.choice(list(string.ascii_lowercase),
                                      size=(LENGTH_OF_STRING_LIST, LENGTH_OF_STRING))

    # Convert the matrix of characters generated by numpy into
    #   an actual list of strings
    string_list = ["".join(row_of_chars) for row_of_chars in char_matrix]
    # Made a copy of the list for sorting with parallel version of quicksort later
    string_list_copy = copy.deepcopy(string_list)

    # Let user know that we're generating the reference sorted list
    #   for validating correctness of each of my implementations of
    #   quicksort
    print("\nGenerating reference sorted list using Python's \"sorted\" "
          "built-in function for\n    validating correctness of serial "
          "version and parallel version of quicksort... ", end="")

    # run reference sort algorithm (i.e. Python's "sorted" function) on
    #     a copy of the randomly generated list of strings to generate
    #     the reference sorted list
    reference_sorted_list = sorted(string_list)

    # Notify user we're done generating reference list.
    print("Done!")

    # Let user know that timing how long each of my personal implementations
    #     of quicksort runs starts now.
    print(f"\nTime to sort list of {LENGTH_OF_STRING_LIST} strings "
          f"where each string is {LENGTH_OF_STRING} characters long...\n")

    # Run personal implementation of serial quicksort on
    #   on a copy of the same list and time how long the
    #   implementation takes to run
    start_sort_time = time.time()
    serial_quicksort(a_list=string_list)
    end_sort_time = time.time()

    # Report time took to run serial quicksort to user
    print(f"...using serial version of quicksort: "
          f"{end_sort_time - start_sort_time:.6f} seconds.\n")

    # Run personal implementation of parallel quicksort on
    #   on a copy of the same list and time how long the
    #   implementation takes to run
    start_sort_time = time.time()
    # Create inter-process communications pipe for retrieving
    #     sorted list result from parent process that calls
    #     the parallel quicksort function.
    receive_sorted_list_socket, send_sorted_list_socket = \
        multiprocessing.Pipe(duplex=False)
    # Create parent process for calling the parallel quicksort function;
    #     the "1" stands for "currently one process has been created for
    #     executing parallel quicksort."
    quicksort_parent_process = multiprocessing.Process(target=parallel_quicksort,
                                                       args=(string_list_copy,
                                                             send_sorted_list_socket,
                                                             1,
                                                             multiprocessing.cpu_count()))
    # Start the process
    quicksort_parent_process.start()
    # Fetch the sorted list from the process which called the
    #     parallel quicksort function; no need to close
    #     the pipe used to receive the list here because
    #     the "parallel_quicksort" function closes the
    #     pipe after it sends over the sorted list.
    string_list_copy = receive_sorted_list_socket.recv()
    # Wait until the parent process has finished transmitting the
    #     sorted list.
    quicksort_parent_process.join()
    end_sort_time = time.time()

    # Report time took to run parallel quicksort to user
    print(f"...using parallel version of quicksort\n"
          f"    (parallelized over a target of "
          f"{multiprocessing.cpu_count()} processes): "
          f"{end_sort_time - start_sort_time:.6f} seconds.\n")

    # Report to user that it is validating the result of personal implementation
    #   of serial quicksort
    print("Validating result of serial version of quicksort...")

    numpy.testing.assert_array_equal(numpy.array(string_list),
                                     numpy.array(reference_sorted_list),
                                     err_msg="serial version of quicksort " \
                                             "did not produce a correctly sorted list.")

    # If an assertion error was not raised, print message of congratulations
    print("    Congratulations, expected and actual lists are equal!\n")

    # Report to user that it is validating the result of personal implementation
    #   of parallel quicksort
    print("Validating result of parallel version of quicksort...")

    numpy.testing.assert_array_equal(numpy.array(string_list_copy),
                                     numpy.array(reference_sorted_list),
                                     err_msg="parallel version of quicksort "
                                             "did not produce a correctly sorted list.")

    # If an assertion error was not raised, print message of congratulations
    print("    Congratulations, expected and actual lists are equal!\n")


# END FUNCTION DECLARATIONS SECTION #


if __name__ == "__main__":
    main()
