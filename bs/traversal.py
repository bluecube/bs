import collections
import concurrent.futures

def update(targets, dirty, job_count = 1):
    need_update = collections.Counter() # node: number of blockers
    new_dirty = set()

    targets = set(targets)

    # A small trick to avoid code duplication -- make a fake node to start checks from
    class Dummy:
        def __str__(self):
            return "(parameter)"
    dummy = Dummy()
    dummy.reverse_dependencies = dirty
    to_check = collections.deque([dummy])

    while to_check:
        node = to_check.popleft()
        for reverse_dependency in node.reverse_dependencies:
            if reverse_dependency.targets.isdisjoint(targets):
                print("Dirty:", reverse_dependency, reverse_dependency.targets)
                new_dirty.add(reverse_dependency)
            else:
                need_update[reverse_dependency] += 1
                print("need update:", reverse_dependency, "caused by", node, "count", need_update[reverse_dependency])
                to_check.append(reverse_dependency)

    print()

    with concurrent.futures.ThreadPoolExecutor(job_count) as executor:
        waiting = set()

        def maybe_submit(node):
            def wrapper(node):
                node.update()
                return node

            if node not in need_update:
                print("not in need_update", node)
                node.update()
                return

            need_update[node] -= 1
            if need_update[node] == 0:
                print("submitting", node)
                del need_update[node]
                waiting.add(executor.submit(wrapper, node))
            else:
                print("still blocked", node, need_update[node])

        # Remove dependency on the dummy node and run the initial updates
        for node in dirty:
            maybe_submit(node)

        while waiting:
            done, waiting = concurrent.futures.wait(waiting,
                                                    return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                node = future.result()
                for reverse_dependency in node.reverse_dependencies:
                    maybe_submit(reverse_dependency)

        return new_dirty #TODO: What if build fails?
