package DirDB;

require 5.005_62;
use strict;
use warnings;
use Carp;

our $VERSION = '0.04';

# @ISA = qw(Tie::Array);



# Preloaded methods go here.


# =pod
#DirDB is a package that lets you access a directory
#as a hash. 
#
# subdirectories become references to
#	  tied objects of this type
#
#pipes and so on are opened for reading and read from
#on FETCH, and clobbered on STORE.  This may change
#but not immediately.
#
#
# =cut

sub TIEHASH {
	my $self = shift;
	my $rootpath = shift or croak "we need a rootpath";
	$rootpath =~ s#/$##; # lose trailing slash for MacOS
	-d $rootpath or
	   mkdir $rootpath, 0777 or
	     croak "could not create dir $rootpath: $!";


	$rootpath .= '/';
	bless \$rootpath, $self;
};

sub TIEARRAY {

	confess "DirDB does not support arrays, use DirDB::Array";

#	my $self = shift;
#	my $rootpath = shift or croak "we need a rootpath";
#	$rootpath =~ s#/$##; # lose trailing slash for MacOS
#	-d $rootpath or
#	   mkdir $rootpath, 0777 or
#	     croak "could not create dir $rootpath: $!";
#
#
#	$rootpath .= '/';
#	my $Object = bless \$rootpath, $self;
#	$Object->STOREMETA('ARRAY', 0);
#	return $Object;
};



sub EXISTS {
	my $rootpath = ${+shift};
	my $key = shift;
	$key =~ s/^ /  /; #escape leading space into two spaces
	# defined (my $key = shift) or return undef;
	$key eq '' and $key = ' EMPTY';
	-e "$rootpath$key";
};

sub FETCH {
	my $ref = shift;
	defined (my $rootpath = $$ref) or croak "undefined rootpath";
	my $key = shift;
	$key =~ s/^ /  /; #escape leading space into two spaces
	# defined (my $key = shift) or return undef;
	$key eq '' and $key = ' EMPTY';
	-e "$rootpath$key" or return undef;
	if(-d "$rootpath$key"){

		#if -e ("$rootpath ARRAY"{
		#	tie my @array, ref($ref),"$rootpath$key";
 		#	return \@array;
		#};
 
		tie my %hash, ref($ref),"$rootpath$key";
 		return \%hash;
	

	};

	open FSDBFH, "<$rootpath$key"
	   or croak "cannot open $rootpath$key: $!";
	join '', (<FSDBFH>);
};

sub STORE {
#	my $ref = shift;
#	my $rootpath = $$ref; # $rootpath = ${shift}
#			      # apparently worked as a cast instead
#			      # of a dereference?

	my $rootpath = ${+shift}; # RTFM! :)

	
	my $key = shift;
	$key =~ s/^ /  /; #escape leading space into two spaces
	$key eq '' and $key = ' EMPTY';
	my $value = shift;
	ref $value and croak "This hash does not support storing references";
	open FSDBFH,">$rootpath${$}TEMP$key" or croak $!;
	print FSDBFH $value;
	close FSDBFH;
	rename "$rootpath${$}TEMP$key", "$rootpath$key" or croak $!;
};

sub FETCHMETA {
	my $ref = shift;
	defined (my $rootpath = $$ref) or croak "undefined rootpath";
	my $key = ' '.shift;
	-e "$rootpath$key" or return undef;
	if(-d "$rootpath$key"){

		confess "Complex metadata not supported in DirDB version $VERSION";	

	};

	open FSDBFH, "<$rootpath$key"
	   or croak "cannot open $rootpath$key: $!";
	join '', (<FSDBFH>);
};

sub STOREMETA {
	my $rootpath = ${+shift}; # RTFM! :)
	my $key = ' '.shift;
	my $value = shift;
	ref $value and croak "DirDB does not support storing references in metadata at version $VERSION";
	open FSDBFH,">$rootpath${$}TEMP$key" or croak $!;
	print FSDBFH $value;
	close FSDBFH;
	rename "$rootpath${$}TEMP$key", "$rootpath$key" or croak $!;
};

sub DELETE {
	my $rootpath = ${+shift};
	my $key = shift;
	$key =~ s/^ /  /; #escape leading space into two spaces
	$key eq '' and $key = ' EMPTY';
	if(-d "$rootpath$key"){
		rmdir "$rootpath$key"
		   or croak "could not delete directory $rootpath$key: $!";
		return "$rootpath$key";
	};
	-e "$rootpath$key" or return undef;

	open FSDBFH, "<$rootpath$key"
	   or croak "cannot open $rootpath$key: $!";
	my $value = join '', (<FSDBFH>);
	unlink "$rootpath$key";
	$value;
};

sub CLEAR{
	my $ref = shift;
	my $path = $$ref;
	opendir FSDBFH, $path or croak "opendir $path: $!";
	while(defined(my $entity = readdir FSDBFH )){
		$entity =~ /^\.\.?\Z/ and next;
		$entity = join('',$path,$entity);
		if(-d $entity){
		 rmdir "$entity"
		   or croak "could not delete (sub-container?) directory $entity: $!";
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
       delete $IteratorListings{shift};
   };
 
};




1;
__END__

=head1 NAME

DirDB - Perl extension to use a directory as a database

=head1 SYNOPSIS

  use DirDB;
  tie my %session, 'DirDB', "./data/session";

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

DirDB croaks on attempts to store references. There are
hooks in place as of version 0.04 to do something more sensible, 
like recursively blatting out the whole object tree being
referred to, but to prevent DirDB being involved in the
possible endless loop problems, implementing such behavior is
left for subclassed to do.

DirDB will croak if it can't open an existing file system
entity, so wrap your fetches in eval blocks if there are
possibilities of permissions problems.  Or better yet rewrite
it into DirDB::nonfragile and publish that.

subdirectories become references to tied objects of this type,
but this is a read-only function at this time.

pipes and so on are opened for reading and read from
on FETCH, and clobbered on STORE.  This may change
but not immediately.

The underlying object is a scalar containing the path to 
the directory.  Keys are names within the directory, values
are the contents of the files.

If anyone cares to benchmark DirDB on ReiserFS against
Berkeley DB for databases of verious sizes, please send me
the results and I will include them here.

STOREMETA and FETCHMETA methods are provided for subclasses
who which to store and fetch metadata (such as array size)
which will not appear in the data returned by NEXTKEY and which
cannot be accessed directly through STORE or FETCH.  DirDB::Array
will store its size and so on using these methods.


=head2 EXPORT

None by default.


=head1 AUTHOR

David Nicol, davidnicol@cpan.org

=head1 Assistance

QA provided by members of Kansas City Perl Mongers, including
Andrew Moore and Craig S. Cottingham.

=head1 LICENSE

GPL

=head1 SEE ALSO

better read <l perltie> before trying to extend this

GPL

=cut
