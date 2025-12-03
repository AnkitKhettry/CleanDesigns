from abc import abstractmethod, ABC


class IterationStrategy(ABC):
    @abstractmethod
    def iterate(self):
        raise NotImplementedError()


class FilterStrategy(ABC):
    @abstractmethod
    def apply(self, ds):
        raise NotImplementedError()


class DataStructure(ABC):

    @abstractmethod
    def get_iteration_strategy(self):
        raise NotImplementedError()

    def filter(self, filter_strategy: FilterStrategy):
        return filter_strategy.apply(self)

    def traverse(self):
        iterator_clazz = self.get_iteration_strategy()
        return iterator_clazz(self).iterate()

    def __iter__(self):
        return self.traverse()


class Node:
    def __init__(self, data, next = None, prev = None):
        self.data = data
        self.next = next
        self.prev = prev

    def __str__(self):
        return str(self.data)


class LinkedList(DataStructure):
    def __init__(self, head=None, iteration_strategy = None):
        self.head = head
        self.tail = head
        self.iteration_strategy = iteration_strategy or ForwardIterationStrategy

    def get_iteration_strategy(self):
        return self.iteration_strategy

    def __str__(self):
        ret_arr = []
        curr = self.head
        while curr:
            ret_arr.append(curr.data)
            curr = curr.next
        return str(ret_arr)


class LinkedListFilterStrategy(FilterStrategy):
    @abstractmethod
    def apply(self, linked_list: LinkedList) -> LinkedList:
        raise NotImplementedError()


class OddNumberFilterStrategy(LinkedListFilterStrategy):
    def apply(self, linked_list: LinkedList):
        builder = LinkedListBuilder()
        curr = linked_list.head
        while curr:
            if curr.data % 2 == 1:
                builder.with_next(curr.data)
            curr = curr.next
        return builder.build()


class EvenNumberFilterStrategy(LinkedListFilterStrategy):
    def apply(self, linked_list: LinkedList):
        builder = LinkedListBuilder()
        curr = linked_list.head
        while curr:
            if curr.data % 2 == 0:
                builder.with_next(curr.data)
            curr = curr.next
        return builder.build()


class ForwardIterationStrategy(IterationStrategy):
    def __init__(self, linked_list):
        self.linked_list = linked_list

    def __iter__(self):
        return self.iterate()

    def iterate(self):
        curr = self.linked_list.head
        while curr:
            yield curr
            curr = curr.next


class BackwardIterationStrategy(IterationStrategy):

    def __init__(self, linked_list):
        self.linked_list = linked_list

    def __iter__(self):
        return self.iterate()

    def iterate(self):
        curr = self.linked_list.tail
        while curr:
            yield curr
            curr = curr.prev


class LinkedListBuilder:
    def __init__(self, linked_list: LinkedList = None):
        self.linked_list = linked_list or LinkedList()

    def with_next(self, value):
        new_node = Node(value)
        if not self.linked_list.head:
            self.linked_list.head = new_node
            self.linked_list.tail = new_node
        else:
            self.linked_list.tail.next = new_node
            new_node.prev = self.linked_list.tail
            self.linked_list.tail = self.linked_list.tail.next
        return self

    def with_prev(self, value):
        new_node = Node(value)
        if not self.linked_list.head:
            self.linked_list.head = new_node
            self.linked_list.tail = new_node
        else:
            self.linked_list.head.prev = new_node
            new_node.next = self.linked_list.head
            self.linked_list.head = self.linked_list.head.prev
        return self

    def build(self):
        return self.linked_list


if __name__ == "__main__":

    linked_list = LinkedListBuilder().with_next(3).with_next(4).with_next(5).with_prev(2).with_prev(1).build()

    print("\nLinked list:")
    print(linked_list)

    print("\nNew linked list with only odd numbers from original:")
    odd_number_filtered_linked_list = linked_list.filter(OddNumberFilterStrategy())
    for node in odd_number_filtered_linked_list:
        print(node)

    print("\nNew linked list with only even numbers from original:")
    even_number_filtered_linked_list = linked_list.filter(EvenNumberFilterStrategy())
    for node in even_number_filtered_linked_list:
        print(node)

    linked_list.iteration_strategy = ForwardIterationStrategy
    print("\nForward iteration:")
    for node in linked_list:
        print(node.data)

    linked_list.iteration_strategy = BackwardIterationStrategy
    print("\nBackward iteration:")
    for node in linked_list:
        print(node)
