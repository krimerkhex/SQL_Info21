create type Status as enum ('Start', 'Success', 'Failure');

create table Peers
(
    Nickname varchar primary key,
    Birthday date default current_date not null
);

create table if not exists Tasks
(
    Title varchar primary key,
    ParentTask varchar references Tasks(Title),
    MaxXP int not null default 0
);

create table Checks
(
    ID serial primary key,
    Peer varchar references Peers(Nickname) not null,
    Task varchar references Tasks(Title) not null,
    Date date default current_date not null
);

create table P2P
(
    ID serial primary key,
    CheckID int references Checks(ID) not null,
    CheckingPeer varchar references Peers(Nickname) not null,
    Status Status not null,
    Time time default current_time not null,
    constraint uk_task_p2p unique (CheckID, CheckingPeer, Status)
);

create table Verter
(
    ID serial primary key,
    CheckID int references Checks(ID) not null,
    Status Status not null,
    Time time default current_time not null
);

create table TransferredPoints
(
    ID serial primary key,
    CheckingPeer varchar references Peers(Nickname) not null,
    CheckedPeer varchar references Peers(Nickname) check (CheckingPeer <> CheckedPeer) not null,
    PointsAmount int default 0 not null
);

create table Friends
(
    ID serial primary key,
    Peer1 varchar references Peers(Nickname) not null,
    Peer2 varchar references Peers(Nickname) check (Peer1 <> Peer2) not null
);

create table Recommendations
(
    ID serial primary key,
    Peer varchar references Peers(Nickname) not null,
    RecommendedPeer varchar references Peers(Nickname) check (Peer <> RecommendedPeer) not null
);

create table XP
(
    ID serial primary key,
    CheckID int references Checks(ID),
    XPAmount int not null check (XPAmount > 0)
);

create table TimeTracking
(
    ID serial primary key,
    Peer varchar references Peers(Nickname),
    Date date default current_date not null,
    Time time default current_time not null,
    State int not null check (State in (1, 2)),
    constraint uk_task_visit unique (Peer, Date, Time)
);
