import collections
import concurrent.futures
import logging

#logger = logging.getLogger(__name__)

def update(context, targets, dirty, job_count = 1):
    need_update = collections.Counter() # node: number of blockers

    targets = set(targets)

    class Head:
        def __str__(self):
            return "HEAD"
    head = Head()
    head.reverse_dependencies = list(dirty)[:]
    to_check = collections.deque([head])

    while to_check:
        node = to_check.popleft()
        for reverse_dependency in node.reverse_dependencies:
            if not reverse_dependency.targets.isdisjoint(targets):
                if reverse_dependency not in need_update:
                    to_check.append(reverse_dependency)
                need_update[reverse_dependency] += 1
                #logger.debug("need update: %s caused by %s count %d", reverse_dependency, node, need_update[reverse_dependency])
            new_dirty.add(reverse_dependency)

    #logger.debug("need_update: %s", ", ".join("{}: {}".format(str(k), v) for k, v in need_update.items()))

    with concurrent.futures.ThreadPoolExecutor(job_count) as executor:
        waiting = set()

        def maybe_submit(node):
            def wrapper(node):
                node.update(context)
                return node

            if node not in need_update:
                #logger.debug("not in need_update: %s", node)
                node.update(context)
                return

            need_update[node] -= 1
            if need_update[node] == 0:
                #logger.debug("submitting: %s", node)
                del need_update[node]
                waiting.add(executor.submit(wrapper, node))
            #else:
                #logger.debug("still blocked: %s (%d)", node, need_update[node])

        # Remove dependency on the head node and run the initial updates
        for node in head.reverse_dependencies:
            maybe_submit(node)

        while waiting:
            done, waiting = concurrent.futures.wait(waiting,
                                                    return_when=concurrent.futures.FIRST_COMPLETED)
            for future in done:
                node = future.result()
                for reverse_dependency in node.reverse_dependencies:
                    maybe_submit(reverse_dependency)

        return new_dirty #TODO: What if build fails?
