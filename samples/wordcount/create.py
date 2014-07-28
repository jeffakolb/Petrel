import randomsentence
import splitsentence
import wordcount

from petrel.emitter import JavaSpout

def create(builder):
    #builder.setSpout("spout", randomsentence.RandomSentenceSpout(), 1)
    builder.setJavaSpout("spout", JavaSpout("storm.petrel.dummy.ActivityStreamsSampleSpout", ['sentence']), 1)
    builder.setBolt("split", splitsentence.SplitSentenceBolt(), 1).shuffleGrouping("spout")
    builder.setBolt("count", wordcount.WordCountBolt(), 1).fieldsGrouping("split", ["word"])
