#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use Test;
BEGIN { plan tests => 9 };
use DirDB;
ok(1); # If we made it this far, we're ok.

use strict;

# tie my %dcty, 'DirDB', ".";
tie my %dcty, 'DirDB', './test_dir';

ok(2);

# print  "\nSTORE TEST\n";
$dcty{pid} = $$;
$dcty{+time} = time;
$dcty{"time"} = time;

# print "\nFETCH TEST\n";
$dcty{pid} == $$ or die "pid did not store and recover";
$dcty{"time"} == time or die "time did not store and recover";

ok(3);

# print "\nEACH TEST\n";
while (my ($k,$v) = each %dcty ) {
  # print "got key <$k>\n";
  # print qq( $k -> $v\n );
}

# print "\nKEYS TEST\n";
for my $f ( keys %dcty ) {
  # print "got key <$f>\n";
  # print qq( $f -> $dcty{ $f }\n );
}

# print "\nKeys now @{[keys %dcty]}\n";
# print "\nCLEARING\n";
%dcty = ();

# print "Keys now @{[keys %dcty]}\n";

ok(4);
# print "\nDelete slice test\n";
@dcty{1..5} = qw{fee fi fo fum five};
# print "fi fo? ",delete( @dcty{2,3}),"\n";
ok("fi fo","@{[delete( @dcty{2,3})]}");
# ok(5);

# print "fee fum five? ", (grep {defined $_} @dcty{1..5}),"\n";
ok( "fee fum five", "@{[grep {defined $_} @dcty{1..5}]}");
# ok(6);

my $$x = "reference test\n";
# print $$x;
eval { $dcty{reftest} = $x };
$@ and ok(7);

my %x;
$x{something}='else';
$dcty{X} = \%x;	# should tie %x to dcty/X
$x{fruit} = 'banana';
ok('banana',$dcty{X}->{fruit});


# print "complex delete test\n";
my $href = delete $dcty{X};
# print "href is $href\n";
# print "has keys @{[keys %$href]}\n";
# print "has values @{[values %$href]}\n";
ok('banana',$href->{fruit});



