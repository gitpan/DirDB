package DirDB;

require 5.005_62;
use strict;
use warnings;
use Carp;

our $VERSION = '0.06';

sub TIEHASH {
	my $self = shift;
	my $rootpath = shift or croak "we need a rootpath";
	$rootpath =~ s#/+$##; # lose trailing slash(es)
	-d $rootpath or
	   mkdir $rootpath, 0777 or
	     croak "could not create dir $rootpath: $!";

	bless \"$rootpath/", $self;
};

sub TIEARRAY {
	confess "DirDB does not support arrays yet";
};

sub TIESCALAR {
	confess "DirDB does not support scalars yet -- try Tie::Slurp";
};


sub EXISTS {
	my $rootpath = ${+shift};
	my $key = shift;
	$key =~ s/^ /  /; #escape leading space into two spaces
	# defined (my $key = shift) or return undef;
	$key eq '' and $key = ' EMPTY';
	-e "$rootpath$key" or -e "$rootpath LOCK$key";
};

sub recursive_delete($);
sub recursive_delete($){
# unlink a file or rm -rf a directory tree
	my $path = shift;
	unless ( -d $path and ! -l $path ){
		unlink $path;
		-e $path and die "Could not unlink [$path]: $!\n";
		return;
	};
	opendir FSDBFH, $path or croak "opendir $path: $!";
	my @DirEnts = (readdir FSDBFH);
	while(defined(my $entity = shift @DirEnts )){
		$entity =~ /^\.\.?\Z/ and next;
		 recursive_delete "$path/$entity";
	};
	rmdir $path or die "could not rmdir [$path]: $!\n";

};

sub FETCH {
	my $ref = shift;
	defined (my $rootpath = $$ref) or croak "undefined rootpath";
	my $key = shift;
	$key =~ s/^ /  /; #escape leading space into two spaces
	# defined (my $key = shift) or return undef;
	$key eq '' and $key = ' EMPTY';
	sleep 1 while -e "$rootpath LOCK$key";
	-e "$rootpath$key" or return undef;
	if(-d "$rootpath$key"){
		tie my %newhash, ref($ref),"$rootpath$key";
 		return \%newhash;
	};

	local *FSDBFH;
	open FSDBFH, "<$rootpath$key"
	   or croak "cannot open $rootpath$key: $!";

	local $/ = undef;
	<FSDBFH>;
};

{
my %CircleTracker;
sub STORE {
	my ($ref , $key, $value) = @_;
	my $rootpath = $$ref;
	# print "Storing $value to $key in $$ref\n";
	my $rnd = join 'X',$$,time,rand(10000);
	
	$key =~ s/^ /  /; #escape leading space into two spaces
	$key eq '' and $key = ' EMPTY';
	my $refvalue = ref $value;
	if ($refvalue){

		if ( $CircleTracker{$value}++ ){
	          croak "$ref version $VERSION cannot store circular structures\n";
		};

		$refvalue eq 'HASH' or	
	          croak 
		   "$ref version $VERSION only stores references to HASH, not $refvalue\n";

		if (tied (%$value)){
			# recursive copy
		 tie my %tmp, ref($ref), "$rootpath TMP$rnd" or
		   croak "tie failed: $!";
		 eval{
		 	# %tmp = %$value

			my ($k,$v);
			while(($k,$v) = each %$value){
				$tmp{$k}=$v;
			};
		 };
		 # print "$rootpath TMP$rnd should now contain @{[%$value]}\n";
		 if($@){
		    my $message = $@;
		    eval {recursive_delete "$rootpath TMP$rnd"};
		    croak "trouble writing [$value] to [$rootpath$key]: $message";

		};
	
		# print "lock (tied)";
		 sleep 1 while !mkdir "$rootpath LOCK$key",0777;
		 {
		  no warnings;
		  rename "$rootpath$key", "$rootpath GARBAGE$rnd"; 
		 };
		 rename "$rootpath TMP$rnd", "$rootpath$key";

		}else{
			# cache, bless, restore
			my @cache = %$value;
			%$value = ();
		# print "lock (untied)";
			while( !mkdir "$rootpath LOCK$key",0777){
				# print "lock conflivt: $!";
				sleep 1;
			};
			{
			 no warnings;
		         rename "$rootpath$key", "$rootpath GARBAGE$rnd";
		        };
		        tie %$value, ref($ref), "$rootpath$key" or
		          warn "tie to [$rootpath$key] failed: $!";
		# print "assignment";
			%$value = @cache;
		};
		
		rmdir "$rootpath LOCK$key";

		delete $CircleTracker{$value};
		# print "GC";
		 eval {recursive_delete "$rootpath GARBAGE$rnd"};
		 if($@){
			croak "GC problem: $@";
		 };
		 return;

	};

	# store a scalar using write-to-temp-and-rename
	local *FSDBFH;
	open FSDBFH,">$rootpath TMP$rnd" or croak $!;
	# defined $value and print FSDBFH $value;
	# this will work under -l without spurious newlines 
	defined $value and syswrite FSDBFH, $value;
	# print FSDBFH qq{$value};
	close FSDBFH;
	rename "$rootpath TMP$rnd" , "$rootpath$key" or
	  croak
	     " could not rename temp file to [$rootpath$key]: $!";
};
};

sub FETCHMETA {
	my $ref = shift;
	defined (my $rootpath = $$ref) or croak "undefined rootpath";
	my $key = ' '.shift;
	-e "$rootpath$key" or return undef;
	if(-d "$rootpath$key"){

		confess "Complex metadata not supported in DirDB version $VERSION";	

	};

	local $/ = undef;
	open FSDBFH, "<$rootpath$key"
	   or croak "cannot open $rootpath$key: $!";
	<FSDBFH>;
};

sub STOREMETA {
	my $rootpath = ${+shift}; # RTFM! :)
	my $key = ' '.shift;
	my $value = shift;
	ref $value and croak "DirDB does not support storing references in metadata at version $VERSION";
	open FSDBFH,">$rootpath${$}TEMP$key" or croak $!;
	defined $value and syswrite FSDBFH, $value;
	# print FSDBFH $value;
	close FSDBFH;
	rename "$rootpath${$}TEMP$key", "$rootpath$key" or croak $!;
};

sub DELETE {
	my $ref = shift;
	my $rootpath = ${$ref};
	my $key = shift;
	$key =~ s/^ /  /; #escape leading space into two spaces
	$key eq '' and $key = ' EMPTY';

	-e "$rootpath$key" or return undef;


	-d "$rootpath$key" and do {
	rename "$rootpath$key", "$rootpath DELETIA$key";

	  if(defined wantarray){
		my %rethash;
		tie my %tmp, ref($ref), "$rootpath DELETIA$key";
		my @keys = keys %tmp;
		my $k;
		for $k (@keys){
			$rethash{$k} = delete $tmp{$k};
		};
		
		eval {recursive_delete "$rootpath DELETIA$key"};
		$@ and croak "could not delete directory $rootpath$key: $@";
		return \%rethash;
		
	  }else{
		eval {recursive_delete "$rootpath DELETIA$key"};
		$@ and croak "could not delete directory $rootpath$key: $@";
		return {};
	  };
	};

	my $value;
	if(defined wantarray){
		local $/ = undef;
		open FSDBFH, "<$rootpath$key";
		$value = <FSDBFH>;
	};
	unlink "$rootpath$key";
	$value;
};

sub CLEAR{
	my $ref = shift;
	my $path = $$ref;
	opendir FSDBFH, $path or croak "opendir $path: $!";
	my @ents = (readdir FSDBFH );
	while(defined(my $entity = shift @ents )){
		$entity =~ /^\.\.?\Z/ and next;
		$entity = join('',$path,$entity);
		if(-d $entity){
		   eval {recursive_delete $entity};
		   $@ and  croak "could not delete (sub-container?) directory $entity: $@";
		};
		unlink $entity;
	};
};

{

   my %IteratorListings;

   sub FIRSTKEY {
	my $ref = shift;
	my $path = $$ref;
	opendir FSDBFH, $path or croak "opendir $path: $!";
	$IteratorListings{$ref} = [ grep {!($_ =~ /^\.\.?\Z/)} readdir FSDBFH ];

	#print "Keys in path <$path> will be shifted from <@{$IteratorListings{$ref}}>\n";
	
	$ref->NEXTKEY;
   };

   sub NEXTKEY{
	my $ref = shift;
	#print "next key in path <$$ref> will be shifted from <@{$IteratorListings{$ref}}>\n";
	@{$IteratorListings{$ref}} or return undef;
	my $key = shift @{$IteratorListings{$ref}};
	if ($key =~ s/^ //){
		if ($key = m/^ /){
			# we have unescaped a leading space.
		}elsif ($key eq 'EMPTY'){
			$key = ''
		#}elsif($key eq 'REF'){
		# 	return $ref->NEXTKEY();	# next
		#}elsif($key =~ m/^ARRAY){
		# 	return $ref->NEXTKEY();	# next
		}else{
			# per-container metadata does not
			# appear in iterations through data.
			return $ref->NEXTKEY();	# next
		}
	};
	wantarray or return $key;
	return @{[$key, $ref->FETCH($key)]};
   };
   
   sub DESTROY{
       delete $IteratorListings{$_[0]};
   };
 
};




1;
__END__

=head1 NAME

DirDB - Perl extension to use a directory as a database

=head1 SYNOPSIS

  use DirDB;
  tie my %session, 'DirDB', "./data/session";
  $session{$sessionID} -> {email} = get_emailaddress();

=head1 DESCRIPTION

DirDB is a package that lets you access a directory
as a hash. The final directory will be created, but not
the whole path to it.

The empty string, used as a key, will be translated into
' EMPTY' for purposes of storage and retrieval.  File names
beginning with a space are reserved for metadata for subclasses,
such as object type or array size or whatever.  Key names beginning
with a space get an additional space prepended to the name
for purposes of naming the file to store that value.

As of version 0.05, DirDB can store hash references. references
to tied hashes are recursively copied, references to plain
hashes are first tied to DirDB and then recursively copied. Storing
a circular hash reference structure will cause DirDB to croak.

As of version 0.06, DirDB now recursively copies subdirectory contents
into an in-memory hash and returns a reference to that hash when
a previously stored hash reference is deleted in non-void context.

DirDB will croak if it can't open an existing file system
entity.

 tie my %d => DirDB, '/tmp/foodb';
 
 $d{ref1}->{ref2}->{ref3}->{ref4} = 'something'; 
 # 'something' is now stored in /tmp/foodb/ref1/ref2/ref3/ref4
 
 my %e = (1 => 2, 2 => 3);
 $d{e} = \%e;
 # %e is now tied to /tmp/foodb/e, and 
 # /tmp/foodb/e/1 and /tmp/foodb/e/2 now contain 2 and 3, respectively

 $d{f} = \%e;
 # like `cp -R /tmp/foodb/e /tmp/foodb/f`

 $e{destination} = 'Kashmir';
 # sets /tmp/foodb/e/destination
 # leaves /tmp/foodb/f alone
 
 my %g = (1 => 2, 2 => 3);
 $d{g} = {%g};
 # %g has been copied into /tmp/foodb/g/ without tying %g.
 
Pipes and so on are opened for reading and read from
on FETCH, and clobbered on STORE. 

The underlying object is a scalar containing the path to 
the directory.  Keys are names within the directory, values
are the contents of the files.


STOREMETA and FETCHMETA methods are provided for subclasses
who which to store and fetch metadata (such as array size)
which will not appear in the data returned by NEXTKEY and which
cannot be accessed directly through STORE or FETCH.


=head2 RISKS

"mkdir locking" is used to protect incomplete directories
from being accessed while they are being written. It is conceivable
that your program might catch a
signal and die while inside a critical section.  If this happens,
a simple 

    find /your/data -type d -name ' LOCK*'

at the command line will identify what you need to delete.


=head2 EXPORT

None by default.


=head1 AUTHOR

David Nicol, davidnicol@cpan.org

=head1 Assistance

version 0.04 QA provided by members of Kansas City Perl Mongers, including
Andrew Moore and Craig S. Cottingham.

=head1 LICENSE

GPL/Artistic (the same terms as Perl itself)

=head1 SEE ALSO

better read <l perltie> before trying to extend this



GPL

=cut
