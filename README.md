# Caskdb - Key/Value Storage

This is a simple Key/Value implementation based in the "Bitcask - A Log-Structured Hash Table for Fast Key/Value Data" of [basho](http://basho.com/wp-content/uploads/2015/05/bitcask-intro.pdf)

Block1 + Block2 + Block3 + Active Block

## Records Structurs
R3C + Sz Len Key + Sz Len Data + Len Key + Len Data  + Key + Data + \n
