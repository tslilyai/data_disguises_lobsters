class DeleteNotification < ApplicationMailer
  def notify(user, did)
    @did = did 
    @uid = user.id

    mail(
      :to => user.email,
      :subject => "[#{Rails.application.name}] Your acount has been deleted"
    )
  end
end
