from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy import (Column, Date, DateTime, Enum, Float, LargeBinary,
                        ForeignKey, String, Text, text)
from sqlalchemy.dialects.mysql import INTEGER, MEDIUMTEXT, TINYINT, LONGTEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import create_session
from sqlalchemy.ext.automap import automap_base
import sqlalchemy as sqa



host = 'hostname'
src_engine = create_engine("mysql+pymysql://root:secret@host/src")
dest_engine = create_engine("mysql+pymysql://root:secret@host/dest")
DELETE_OLD = True


src_Base = automap_base()
src_Base.prepare(src_engine, reflect = True)
src_session = create_session(bind = src_engine)

src_compute_nodes = src_Base.classes.compute_nodes
src_services = src_Base.classes.services
src_instances = src_Base.classes.instances
src_block_device_mapping = src_Base.classes.block_device_mapping
src_instance_actions = src_Base.classes.instance_actions
src_instance_extra = src_Base.classes.instance_extra
src_instance_faults = src_Base.classes.instance_faults
src_instance_info_caches = src_Base.classes.instance_info_caches
src_instance_metadata = src_Base.classes.instance_metadata
src_instance_system_metadata = src_Base.classes.instance_system_metadata
src_virtual_interfaces = src_Base.classes.virtual_interfaces
src_instance_actions_events = src_Base.classes.instance_actions_events

dest_Base = automap_base()
dest_Base.prepare(dest_engine, reflect = True)

dest_session = create_session(bind = dest_engine)

Session = sessionmaker(bind=dest_engine)
dest_session = Session()

dest_compute_nodes = dest_Base.classes.compute_nodes
dest_services = dest_Base.classes.services
dest_instances = dest_Base.classes.instances
dest_block_device_mapping = dest_Base.classes.block_device_mapping
dest_instance_actions = dest_Base.classes.instance_actions
dest_instance_extra = dest_Base.classes.instance_extra
dest_instance_faults = dest_Base.classes.instance_faults
dest_instance_info_caches = dest_Base.classes.instance_info_caches
dest_instance_metadata = dest_Base.classes.instance_metadata
dest_instance_system_metadata = dest_Base.classes.instance_system_metadata
dest_virtual_interfaces = dest_Base.classes.virtual_interfaces
dest_instance_actions_events = dest_Base.classes.instance_actions_events

def DuplicateObject(oldObj):
    # SQLAlchemy related data class?
    if not isinstance(oldObj, src_Base):
        raise TypeError('The given parameter with type {} is not ' \
            'mapped by SQLAlchemy.'.format(type(oldObj)))

    mapper = sqa.inspect(type(oldObj))
    newObj = type(oldObj)()

    for name, col in mapper.columns.items():
        # no PrimaryKey
        if not col.primary_key:
            setattr(newObj, name, getattr(oldObj, name))

    return newObj


def copy_data(q, delete_old=False):
    for row in q.all():
        new = DuplicateObject(row)
        dest_session.add(new)
        if delete_old:
            src_session.delete(row)
            src_session.flush()
    dest_session.commit()

q = src_session.query(src_compute_nodes).filter(src_compute_nodes.host == host).filter(src_compute_nodes.deleted == 0)
copy_data(q, DELETE_OLD)

q = src_session.query(src_services).filter(src_services.host == host).filter(src_services.deleted == 0)
copy_data(q, DELETE_OLD)
    
q1 = src_session.query(src_instances).filter(src_instances.host == host)
copy_data(q1)

q = src_session.query(src_block_device_mapping).join(src_instances,(src_instances.uuid == src_block_device_mapping.instance_uuid)).filter(src_instances.host == host)
copy_data(q, DELETE_OLD)

q = src_session.query(src_instance_faults).join(src_instances,(src_instances.uuid == src_instance_faults.instance_uuid)).filter(src_instances.host == host)
copy_data(q, DELETE_OLD)

q = src_session.query(src_instance_extra).join(src_instances,(src_instances.uuid == src_instance_extra.instance_uuid)).filter(src_instances.host == host)
copy_data(q, DELETE_OLD)

q = src_session.query(src_instance_info_caches).join(src_instances,(src_instances.uuid == src_instance_info_caches.instance_uuid)).filter(src_instances.host == host)
copy_data(q, DELETE_OLD)

q = src_session.query(src_instance_metadata).join(src_instances,(src_instances.uuid == src_instance_metadata.instance_uuid)).filter(src_instances.host == host)
copy_data(q, DELETE_OLD)

q = src_session.query(src_instance_system_metadata).join(src_instances,(src_instances.uuid == src_instance_system_metadata.instance_uuid)).filter(src_instances.host == host)
copy_data(q, DELETE_OLD)

q = src_session.query(src_virtual_interfaces).join(src_instances,(src_instances.uuid == src_virtual_interfaces.instance_uuid)).filter(src_instances.host == host)
copy_data(q, DELETE_OLD)


def copy_events_data(q, delete_old=False):
    for row in q.all():
        new = DuplicateObject(row)
        dest_session.add(new)
        dest_session.flush()
        new_id = new.id
        old_id = row.id
        q2 = src_session.query(src_instance_actions_events).filter(src_instance_actions_events.action_id == old_id)
        for old_event in q2.all():
           new_event = DuplicateObject(old_event)
           new_event.action_id = new_id
           dest_session.add(new_event)
        dest_session.commit()  
        if delete_old:
            q2.delete()
            src_session.delete(row)
            src_session.flush()


q = src_session.query(src_instance_actions).join(src_instances,(src_instances.uuid == src_instance_actions.instance_uuid)).filter(src_instances.host == host)
copy_events_data(q, DELETE_OLD)

if DELETE_OLD:
    q1.delete()
