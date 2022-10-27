import jsonlines


def convert_to_json(docs, outfile):

    with jsonlines.Writer(outfile) as writer:
        writer.write_all(docs)
