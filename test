#!/bin/bash
#set -x
#set -e
test_dir=/disks/ssdbtrfs/bucko/test
script=/home/bucko/btrfs-receive-go/btrfs-receive-go
mkdir -p "$test_dir"
cd "$test_dir"
[ -d rw ] && btrfs subvolume delete rw > /dev/null
[ -n "$(ls snaps)" ] && for X in snaps/*; do
	btrfs subvolume delete "$X" > /dev/null
done
btrfs subvolume create rw > /dev/null
mkdir -p snaps
btrfs subvolume snapshot -r rw snaps/000 > /dev/null
I=1
(
cat <<END
echo foo > foo_file
mkdir bar
mv foo_file bar
echo baz > bar/baz_file
ln bar/baz_file bar/baaz_file
mv bar/baz_file bar/foo_file
rm bar/foo_file
rm -rf bar
END
) | while read command; do
	(cd rw; sh -c "$command")
	btrfs subvolume snapshot -r rw snaps/$(printf "%03i" $I) > /dev/null
	I=$(($I + 1))
done
for A in snaps/*; do
	for B in snaps/*; do
		if [ "$A" = "$B" ]; then continue; fi
		"$script" $test_dir/"$A" $test_dir/"$B" > /tmp/a_raw 2>&1
		cat /tmp/a_raw | grep -A 100 GENERATED|grep -v GENERATED|cut -b10-|sort | grep -v '^changed: $' > /tmp/a || true
		diff -qr "$A" "$B" | \
			sed "s|$A|old|; s|$B|new|g; s|: |/|; s/Only in new/  added: /; s/Only in old/deleted: /; s|Files old/.* and new/\(.*\) differ|changed: /\1|" | \
			sort > /tmp/b || true
		# Filter things that were spuriously added (can happen due to utimes changes and stuff).
		# Then filter only changes (else we spit out headers for the stuff we filtered).
		diff -u5 /tmp/a /tmp/b | grep -v '^-changed' | grep '^[+-][^+-]' | sed "s|^|$A $B: |"
	done
done
