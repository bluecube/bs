#!/usr/bin/env python3

import sys
sys.path.append("../../") # To find the bs import on the next line
import bs

class ConcatenateGenerator(bs.Builder):
    """ Example of a custom builder that has multiple inputs and multiple outputs. """

    def __init__(self, context, function_name):
        super().__init__(context)
        self.function_name = function_name;

    def build(self, input_paths, output_paths):
        """ Build input_paths is a list of pathlib.Path objects that should be
        compiled into list of patlib.Path objects output_paths. """

        text = []
        for path in sorted(input_paths):
            with open(str(path), "r") as fp:
                text.append(fp.read().strip())
        text = " ".join(text)

        with output_paths[0].open("w") as fp:
            fp.write("#pragma once\n")
            fp.write("void " + self.function_name + "();\n")

        with output_paths[1].open("w") as fp:
            fp.write("#include <stdio.h>\n")
            fp.write("#include \"" + output_paths[0].name + "\"\n")
            fp.write("void " + self.function_name + "(){\n")
            fp.write("printf(\"" + text + "\\n\");\n")
            fp.write("}\n")

        return [] # No additional dependencies

    def get_output_count(self, input_count):
        """ Return how many files will be generated by this build step
        check for correct number of inputs. """
        return 2

    def get_hash(self):
        """ Hash any parameters of this builder """
        return self.hash_helper([self.function_name])

with bs.Bs() as builder:
    # TODO: Add builder that generates header with date.

    greet_generator = ConcatenateGenerator(builder, "fun");
    generated_h, generated_c = builder.apply(greet_generator,
                                             builder.root.glob("*.txt"),
                                             ["generated.h", None])

    compiler = bs.gcc.GccCompiler(builder);
    compiler.add_dependency(generated_h)
    compiler.cflags.extend(["-I", generated_h.directory])

    ofiles = []
    for f in builder.root.glob("**/*.c"):
        ofiles.extend(builder.apply(compiler, f))
    ofiles.extend(builder.apply(compiler, generated_c))

    builder.add_target(builder.apply(compiler.create_associated_linker(),
                       ofiles,
                       "hello_world"))
