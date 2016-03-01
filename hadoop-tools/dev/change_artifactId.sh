TO_VERSION=1.8

sed_i() {
    sed -e "$1" "$2" > "$2.tmp" && mv "$2.tmp" "$2"
  }

export -f sed_i
                                                                               
BASEDIR=$(dirname $0)/..
find "$BASEDIR" -name 'pom.xml' -not -path '*target*' -print \
  -exec bash -c "sed_i 's/\(<artifactId>hadoop-.*\)\(<\/.*>\)/\1_$TO_VERSION\2/g' {}" \;

