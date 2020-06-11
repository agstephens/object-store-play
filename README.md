# object-store-play
Examples and code to play with Object Stores

## Amazon S3 Range Gets

```
curl -r 0-1024 https://s3.amazonaws.com/mybucket/myobject -o part1
curl -r 1025-  https://s3.amazonaws.com/mybucket/myobject -o part2
cat part1 part2 > myobject
```

or:

```
var s3 = new AWS.S3();
var file = require('fs').createWriteStream('part1');
var params = {
    Bucket: 'mybucket',
    Key: 'myobject',
    Range: 'bytes=0-1024'
};
s3.getObject(params).createReadStream().pipe(file);
```
