awk '$6 == D || $6 == "I" || $6 == "Q" || $6 == "B" {printf "%-15s %s %s %d %d\n", $4, $6, $7, $8, $10}' test.txt