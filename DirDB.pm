package DirDB;

require 5.005_62;
use strict;
use warnings;
use Carp;

our $VERSION = '0.03';


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
	'/' eq substr($rootpath , -1) or $rootpath .= '/';
	-d $rootpath or
	   mkdir $rootpath, 0777 or
	     croak "could not create dir $rootpath: $!";


	bless \$rootpath, $self;
};



sub EXISTS {
	my $rootpath = ${+shift};
	my $key = shift;
	-e "$rootpath$key";
};

sub FETCH {
	my $ref = shift;
	defined (my $rootpath = $$ref) or croak "undefined rootpath";
	defined (my $key = shift) or return undef;
	-e "$rootpath$key" or return undef;
	if(-d "$rootpath$key"){

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
	my $value = shift;
	ref $value and croak "This hash does not support storing references";
	open FSDBFH,">$rootpath${$}TEMP$key" or croak $!;
	print FSDBFH $value;
	close FSDBFH;
	rename "$rootpath${$}TEMP$key", "$rootpath$key" or croak $!;
};

sub DELETE {
	my $rootpath = ${+shift};
	my $key = shift;
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
		   or croak "could not delete directory $entity: $!";
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

DirDB croaks on attempts to store references.

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


=head2 EXPORT

None by default.


=head1 AUTHOR

David Nicol, davidnicol@cpan.org

=head1 Assistance

QA provided by members of Kansas City Perl Mongers, including
Andrew Moore and Craig S. Cottingham.

=head1 LICENSE

GPL

=cut
